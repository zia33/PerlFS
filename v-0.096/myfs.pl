#!/usr/bin/env perl
use strict;
use warnings;

use Fuse;
use POSIX qw(
    ENOENT EEXIST EACCES ENOTEMPTY ENOSYS
    getuid getegid fsync
);
use Fcntl qw(
    ORDONLY OWRONLY ORDWR OCREAT O_TRUNC
);
use File::Spec;
use File::Path qw(make_path);
use JSON::PP;
use Time::HiRes qw(time);
use Cwd qw(realpath);

============================================================

Global config

============================================================

our $DEBUG   = 0;        # set by --debug or --test
our $ROOT;               # backing root directory
our $JOURNAL;            # journal file path

my $UID = getuid();
my $GID = getegid();

============================================================

Utilities

============================================================

sub _log {
    return unless $DEBUG;
    warn "[perlfs] @_ \n";
}

Normalize and constrain paths to stay under $ROOT
sub fs_realpath {
    my ($path) = @_;
    $path //= '/';

    # Strip leading slash, split, normalize ., ..
    $path =~ s{^/}{};
    my @parts = grep { length $_ } split m{/}, $path;
    my @norm;
    for my $p (@parts) {
        if ($p eq '.') {
            next;
        } elsif ($p eq '..') {
            pop @norm if @norm;
        } else {
            push @norm, $p;
        }
    }
    my $joined = @norm ? File::Spec->catfile($ROOT, @norm)
                       : $ROOT;

    # For existing paths, resolve symlinks; for non-existing, just use joined
    my $rp = realpath($joined);
    $rp //= $joined;

    # Enforce that final path is under $ROOT
    my $root_rp = realpath($ROOT) // $ROOT;
    if (index($rp, $root_rp) != 0) {
        die "path escape outside root: $path";
    }

    return $rp;
}

sub parentdir {
    my ($p) = @_;
    return '/' if $p eq '/';
    $p =~ s{/[^/]+$}{};
    return $p eq '' ? '/' : $p;
}

sub isfile { -f fsrealpath($[0]) }
sub isdir  { -d fsrealpath($[0]) }

============================================================

Journaling (length-prefixed JSON WAL)

============================================================

my $JH;

sub journal_open {
    open $JH, '>>', $JOURNAL or die "journal open failed: $!";
    select((select($JH), $|=1)[0]); # autoflush
}

sub journal_record {
    my ($rec) = @_;
    my $json = encode_json($rec);
    my $len  = length($json);
    print $JH "$len\n$json" or die "journal write failed";
    fsync(fileno($JH));  # ensure journal hits disk
}

sub applyjournalrecord {
    my ($r) = @_;
    my $op = $r->{op};

    eval {
        if ($op eq 'create') {
            my $real = fs_realpath($r->{path});
            my $parent = fs_realpath(parentdir($r->{path}));
            make_path($parent) unless -d $parent;
            sysopen(my $fh, $real, OCREAT|ORDWR, $r->{mode}) or die "create: $!";
            close $fh;
        }
        elsif ($op eq 'write') {
            my $real = fs_realpath($r->{path});
            return unless -e $real;
            open my $fh, '+<', $real or die "write open: $!";
            binmode $fh;
            seek $fh, $r->{off}, 0;
            my $w = syswrite($fh, $r->{data});
            die "write failed" unless defined $w;
            fsync(fileno($fh));
            close $fh;
        }
        elsif ($op eq 'truncate') {
            my $real = fs_realpath($r->{path});
            return unless -e $real;
            open my $fh, '+<', $real or die "truncate open: $!";
            truncate($fh, $r->{len}) or die "truncate: $!";
            fsync(fileno($fh));
            close $fh;
        }
        elsif ($op eq 'unlink') {
            my $real = fs_realpath($r->{path});
            unlink($real);
        }
        elsif ($op eq 'mkdir') {
            my $real = fs_realpath($r->{path});
            my $parent = fs_realpath(parentdir($r->{path}));
            make_path($parent) unless -d $parent;
            make_path($real, { mode => $r->{mode} });
        }
        elsif ($op eq 'rmdir') {
            my $real = fs_realpath($r->{path});
            rmdir($real);
        }
        elsif ($op eq 'rename') {
            my $ro = fs_realpath($r->{old});
            my $rn = fs_realpath($r->{new});
            my $parent = fs_realpath(parentdir($r->{new}));
            make_path($parent) unless -d $parent;
            rename($ro, $rn);
        }
        elsif ($op eq 'chmod') {
            my $real = fs_realpath($r->{path});
            chmod($r->{mode}, $real) if -e $real;
        }
        elsif ($op eq 'utime') {
            my $real = fs_realpath($r->{path});
            utime($r->{atime}, $r->{mtime}, $real) if -e $real;
        }
    };
    _log("replay $op failed: $@") if $@;
}

sub replay_journal {
    return unless -e $JOURNAL;

    _log("replaying journal");
    open my $fh, '<', $JOURNAL or die "cannot read journal: $!";

    while (1) {
        my $lenline = <$fh>;
        last unless defined $lenline;
        chomp $lenline;
        last unless $lenline =~ /^\d+$/;

        my $len = $lenline;
        my $json = '';
        my $r = read($fh, $json, $len);
        last unless defined $r && $r == $len;

        my $rec = eval { decode_json($json) };
        if ($@) {
            _log("journal decode failed: $@");
            last;
        }
        applyjournalrecord($rec);
    }

    close $fh;
    # truncate journal after successful replay
    open my $wipe, '>', $JOURNAL; close $wipe;
    _log("journal replay complete");
}

============================================================

FUSE callbacks (15+ operations)

============================================================

sub getattr {
    my ($path) = @_;
    my $real;
    eval { $real = fs_realpath($path) };
    return -ENOENT() if $@;

    my @st = lstat($real);
    return -ENOENT() unless @st;
    return @st;
}

sub readdir {
    my ($path) = @_;
    my $real;
    eval { $real = fs_realpath($path) };
    return -ENOENT() if $@;
    return -ENOENT() unless -d $real;

    opendir(my $dh, $real) or return -EACCES();
    my @e = readdir($dh);
    closedir($dh);
    return @e;
}

sub open {
    my ($path, $flags) = @_;
    my $real;
    eval { $real = fs_realpath($path) };
    return -ENOENT() if $@;
    return -ENOENT() unless -e $real && -f $real;

    my $acc = $flags & O_ACCMODE;
    if ($acc != ORDONLY && $acc != OWRONLY && $acc != O_RDWR) {
        return -EACCES();
    }
    return 0;
}

sub read {
    my ($path, $size, $off) = @_;
    return -ENOENT() unless isfile($path);

    my $real = fs_realpath($path);
    open my $fh, '<', $real or return -EACCES();
    binmode $fh;
    seek $fh, $off, 0;
    my $buf = '';
    my $r = read($fh, $buf, $size);
    close $fh;
    return '' unless defined $r;
    return $buf;
}

sub write {
    my ($path, $buf, $off) = @_;
    return -ENOENT() unless isfile($path);

    journal_record({
        op   => 'write',
        path => $path,
        off  => $off,
        data => $buf,
    });

    my $real = fs_realpath($path);
    open my $fh, '+<', $real or return -EACCES();
    binmode $fh;
    seek $fh, $off, 0;
    my $w = syswrite($fh, $buf);
    fsync(fileno($fh));
    close $fh;
    return defined($w) ? $w : -EACCES();
}

sub create {
    my ($path, $flags, $mode) = @_;

    my $real;
    eval { $real = fs_realpath($path) };
    return -EACCES() if $@;

    return -EEXIST() if -e $real;

    my $parent = fs_realpath(parentdir($path));
    return -ENOENT() unless -d $parent;

    journal_record({ op=>'create', path=>$path, mode=>$mode & 0777 });

    sysopen(my $fh, $real, OCREAT|ORDWR, $mode & 0777) or return -EACCES();
    fsync(fileno($fh));
    close $fh;
    return 0;
}

sub truncate {
    my ($path, $len) = @_;
    return -ENOENT() unless isfile($path);

    journal_record({ op=>'truncate', path=>$path, len=>$len });

    my $real = fs_realpath($path);
    open my $fh, '+<', $real or return -EACCES();
    truncate($fh, $len) or do { close $fh; return -EACCES(); };
    fsync(fileno($fh));
    close $fh;
    return 0;
}

sub unlink {
    my ($path) = @_;
    return -ENOENT() unless isfile($path);

    journal_record({ op=>'unlink', path=>$path });

    my $real = fs_realpath($path);
    unlink($real) or return -EACCES();
    return 0;
}

sub mkdir {
    my ($path, $mode) = @_;

    my $real;
    eval { $real = fs_realpath($path) };
    return -EACCES() if $@;
    return -EEXIST() if -e $real;

    my $parent = fs_realpath(parentdir($path));
    return -ENOENT() unless -d $parent;

    journal_record({ op=>'mkdir', path=>$path, mode=>$mode & 0777 });

    eval { make_path($real, { mode => $mode & 0777 }); };
    if ($@) {
        _log("mkdir failed: $@");
        return -EACCES();
    }
    return 0;
}

sub rmdir {
    my ($path) = @_;

    my $real;
    eval { $real = fs_realpath($path) };
    return -EACCES() if $@;
    return -ENOENT() unless -d $real;

    opendir(my $dh, $real) or return -EACCES();
    my @ents = grep { $ ne '.' && $ ne '..' } readdir($dh);
    closedir($dh);
    return -ENOTEMPTY() if @ents;

    journal_record({ op=>'rmdir', path=>$path });

    rmdir($real) or return -EACCES();
    return 0;
}

sub rename {
    my ($o, $n) = @_;

    my $real_o;
    my $real_n;
    eval {
        $realo = fsrealpath($o);
        $realn = fsrealpath($n);
    };
    return -EACCES() if $@;
    return -ENOENT() unless -e $real_o;

    my $parent = fs_realpath(parentdir($n));
    return -ENOENT() unless -d $parent;

    journal_record({ op=>'rename', old=>$o, new=>$n });

    rename($realo, $realn) or return -EACCES();
    return 0;
}

sub chmod {
    my ($p, $m) = @_;

    my $real;
    eval { $real = fs_realpath($p) };
    return -EACCES() if $@;
    return -ENOENT() unless -e $real;

    journal_record({ op=>'chmod', path=>$p, mode=>$m & 0777 });

    chmod($m, $real) or return -EACCES();
    return 0;
}

sub utime {
    my ($p, $a, $m) = @_;

    my $real;
    eval { $real = fs_realpath($p) };
    return -EACCES() if $@;
    return -ENOENT() unless -e $real;

    journal_record({ op=>'utime', path=>$p, atime=>$a, mtime=>$m });

    utime($a, $m, $real) or return -EACCES();
    return 0;
}

sub chown {
    # Not implemented; let kernel know
    return -ENOSYS();
}

sub statfs {
    # Very rough static values
    my $bsize  = 4096;
    my $blocks = 1000000;
    my $bfree  = 900_000;
    my $files  = 1000000;
    my $ffree  = 900_000;
    my $namelen = 255;
    return ($bsize, $blocks, $bfree, $files, $ffree, $namelen);
}

sub flush   { 0 }
sub release { 0 }

============================================================

Test harness (no FUSE, direct callback calls)

============================================================

sub runtest {
    print "perlfs test harness (direct callbacks). Commands:\n";
    print "  ls PATH\n";
    print "  stat PATH\n";
    print "  cat PATH\n";
    print "  write PATH TEXT...\n";
    print "  mkdir PATH\n";
    print "  rmdir PATH\n";
    print "  rm PATH\n";
    print "  mv OLD NEW\n";
    print "  truncate PATH LEN\n";
    print "  quit\n";

    while (1) {
        print "perlfs> ";
        my $line = <STDIN>;
        last unless defined $line;
        chomp $line;
        next if $line =~ /^\s*$/;

        my ($cmd, @args) = split /\s+/, $line, 3;

        if ($cmd eq 'quit') {
            last;
        } elsif ($cmd eq 'ls') {
            my $p = $args[0] // '/';
            my @ents = readdir($p, 0);
            if (@ents && $ents[0] ne -ENOENT()) {
                print join("  ", @ents), "\n";
            } else {
                print "ls: $p: No such file or directory\n";
            }
        } elsif ($cmd eq 'stat') {
            my $p = $args[0] // '/';
            my @st = getattr($p);
            if (@st && $st[0] != -ENOENT()) {
                print "mode=$st[2] size=$st[7]\n";
            } else {
                print "stat: $p: No such file or directory\n";
            }
        } elsif ($cmd eq 'cat') {
            my $p = $args[0] // '/';
            my $out = read($p, 1<<20, 0);
            if (!defined($out) || $out eq -ENOENT()) {
                print "cat: $p: No such file\n";
            } else {
                print $out;
            }
        } elsif ($cmd eq 'write') {
            my ($p, $text) = @args;
            $text //= '';
            my $rc = write($p, $text, 0);
            print "write rc=$rc\n";
        } elsif ($cmd eq 'mkdir') {
            my $p = $args[0] // '/newdir';
            my $rc = mkdir($p, 0755);
            print "mkdir rc=$rc\n";
        } elsif ($cmd eq 'rmdir') {
            my $p = $args[0] // '/newdir';
            my $rc = rmdir($p);
            print "rmdir rc=$rc\n";
        } elsif ($cmd eq 'rm') {
            my $p = $args[0] // '/file.txt';
            my $rc = unlink($p);
            print "rm rc=$rc\n";
        } elsif ($cmd eq 'mv') {
            my ($old, $new) = @args;
            my $rc = rename($old, $new);
            print "mv rc=$rc\n";
        } elsif ($cmd eq 'truncate') {
            my ($p, $len) = @args;
            my $rc = truncate($p, $len);
            print "truncate rc=$rc\n";
        } else {
            print "Unknown command: $cmd\n";
        }
    }
}

============================================================

Argument parsing / main

============================================================

sub parse_args {
    my @argv = @_;
    my $mode = '';   # '', '--test', '--debug'
    my $root;
    my $mount;

    while (@argv) {
        my $a = shift @argv;
        if ($a eq '--test' || $a eq '--debug') {
            $mode = $a;
        } elsif ($a eq '--root') {
            $root = shift @argv or die "--root requires a path\n";
        } else {
            $mount = $a;
        }
    }

    if ($mode eq '--test') {
        die "Usage: $0 --test --root <backing_root>\n" unless $root;
    } else {
        die "Usage: $0 [--debug] --root <backing_root> <mountpoint>\n"
            unless $root && $mount;
    }

    return ($mode, $root, $mount);
}

my ($mode, $root, $mountpoint) = parse_args(@ARGV);
$ROOT = $root;
die "root '$ROOT' is not a directory\n" unless -d $ROOT;

$JOURNAL = File::Spec->catfile($ROOT, '.perlfs_journal');

Enable debug logging in test/debug modes
$DEBUG = 1 if $mode eq '--debug' || $mode eq '--test';

Recover from crashes
replay_journal();
journal_open();

if ($mode eq '--test') {
    runtest();
    exit 0;
}

die "mountpoint '$mountpoint' is not a directory\n" unless -d $mountpoint;

_log("Starting perlfs on $mountpoint (root=$ROOT)");

Fuse::main(
    mountpoint => $mountpoint,
    getattr    => \&getattr,
    readdir    => \&readdir,
    open       => \&open,
    read       => \&read,
    write      => \&write,
    create     => \&create,
    truncate   => \&truncate,
    unlink     => \&unlink,
    mkdir      => \&mkdir,
    rmdir      => \&rmdir,
    rename     => \&rename,
    chmod      => \&chmod,
    utime      => \&utime,
    chown      => \&chown,
    statfs     => \&statfs,
    flush      => \&flush,
    release    => \&release,
    threaded   => 0,
    debug      => 0,
);

END

##Usage modes:

#1) Test harness (no FUSE, direct callbacks, journaling + recovery):

#perl perlfs.pl --test --root /path/to/backing_root



#2) Normal FUSE mount:

#sudo perl perlfs.pl --root /path/to/backing_root /path/to/mountpoint



#3) Debug FUSE mount (verbose logging to stderr):

#sudo perl perlfs.pl --debug --root /path/to/backing_root /path/to/mountpoint

