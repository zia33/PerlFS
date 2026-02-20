#!/usr/bin/env perl
use strict;
use warnings;

use Fuse;
use POSIX qw(ENOENT EEXIST EACCES ENOTEMPTY ENOSYS);
use Fcntl qw(O_RDONLY O_WRONLY O_RDWR O_CREAT O_TRUNC LOCK_EX LOCK_UN);
use File::Spec;
use File::Path qw(make_path);
use File::Basename qw(dirname);
use JSON::PP;
use Time::HiRes qw(time);
use Digest::CRC qw(crc32);
use Cwd qw(abs_path);

# Define O_ACCMODE if not available
use constant O_ACCMODE => 3;

# ============================================================
# Global config
# ============================================================

our $DEBUG   = 0;        # set by --debug or --test
our $ROOT;               # backing root directory
our $JOURNAL;            # journal file path
our $JOURNAL_LOCK;       # journal lock file
our $JOURNAL_VERSION = 1; # journal format version

my $UID = $<;            # real UID
my ($GID) = split ' ', $(;  # first effective GID only

# Journal limits
use constant MAX_JOURNAL_SIZE => 100 * 1024 * 1024; # 100MB
use constant JOURNAL_BATCH_SIZE => 10;               # operations before fsync

# ============================================================
# Utilities
# ============================================================

sub _log {
    return unless $DEBUG;
    my $msg = join(' ', @_);
    warn "[perlfs] $msg\n";
}

sub _error {
    my $msg = join(' ', @_);
    warn "[perlfs ERROR] $msg\n";
}

# Normalize and constrain paths to stay under $ROOT
sub fs_realpath {
    my ($path) = @_;
    $path //= '/';

    # Strip leading slash, split, normalize ., ..
    $path =~ s{^/+}{};
    my @parts = grep { length $_ } split m{/+}, $path;
    my @norm;
    for my $p (@parts) {
        if ($p eq '.') {
            next;
        } elsif ($p eq '..') {
            pop @norm if @norm;
        } elsif ($p =~ /^\.\./) {
            # Prevent tricks like "..secret"
            return undef;
        } else {
            push @norm, $p;
        }
    }
    
    my $joined = @norm ? File::Spec->catfile($ROOT, @norm) : $ROOT;

    # Get canonical path
    my $canonical;
    if (-e $joined) {
        $canonical = abs_path($joined);
    } else {
        # For non-existent paths, resolve parent and append name
        my $parent_path = dirname($joined);
        if (-e $parent_path) {
            my $parent_real = abs_path($parent_path);
            my $basename = (File::Spec->splitpath($joined))[2];
            $canonical = File::Spec->catfile($parent_real, $basename);
        } else {
            $canonical = $joined;
        }
    }

    return undef unless defined $canonical;

    # Enforce that final path is under $ROOT
    my $root_canonical = abs_path($ROOT);
    return undef unless defined $root_canonical;
    
    # Check if canonical path starts with root path
    if (index($canonical, $root_canonical) == 0) {
        # Additional check: ensure next char is / or end of string
        my $len = length($root_canonical);
        if (length($canonical) == $len || substr($canonical, $len, 1) eq '/') {
            return $canonical;
        }
    }

    _log("path escape attempt: $path -> $canonical");
    return undef;
}

sub parentdir {
    my ($p) = @_;
    return '/' if $p eq '/';
    $p =~ s{/+[^/]+$}{};
    return $p eq '' ? '/' : $p;
}

sub isfile { 
    my $real = fs_realpath($_[0]);
    return 0 unless defined $real;
    return -f $real;
}

sub isdir  { 
    my $real = fs_realpath($_[0]);
    return 0 unless defined $real;
    return -d $real;
}

# ============================================================
# Journaling (length-prefixed JSON WAL with CRC)
# ============================================================

my $JH;
my $LOCK_FH;
my $journal_batch = 0;

sub journal_lock {
    return if $LOCK_FH;
    open $LOCK_FH, '>>', $JOURNAL_LOCK or die "Cannot open journal lock: $!";
    flock($LOCK_FH, LOCK_EX) or die "Cannot lock journal: $!";
}

sub journal_unlock {
    return unless $LOCK_FH;
    flock($LOCK_FH, LOCK_UN);
    close $LOCK_FH;
    undef $LOCK_FH;
}

sub journal_open {
    journal_lock();
    
    open $JH, '>>', $JOURNAL or die "journal open failed: $!";
    binmode $JH;
    select((select($JH), $|=1)[0]); # autoflush
    
    # Check journal size and rotate if needed
    my $size = -s $JOURNAL;
    if (defined $size && $size > MAX_JOURNAL_SIZE) {
        _log("Journal size $size exceeds limit, rotating");
        journal_rotate();
    }
}

sub journal_close {
    if ($JH) {
        close $JH;
        undef $JH;
    }
    journal_unlock();
}

sub journal_rotate {
    return unless -e $JOURNAL;
    
    my $backup = $JOURNAL . '.' . time();
    _log("Rotating journal to $backup");
    
    close $JH if $JH;
    rename($JOURNAL, $backup) or _error("Failed to rotate journal: $!");
    
    # Reopen new journal
    open $JH, '>>', $JOURNAL or die "journal reopen failed: $!";
    binmode $JH;
    select((select($JH), $|=1)[0]);
}

sub journal_record {
    my ($rec) = @_;
    
    $rec->{version} = $JOURNAL_VERSION;
    $rec->{timestamp} = time();
    
    my $json = encode_json($rec);
    my $len  = length($json);
    my $crc  = crc32($json);
    
    journal_lock() unless $LOCK_FH;
    
    print $JH "$len $crc\n$json" or die "journal write failed: $!";
    
    $journal_batch++;
    if ($journal_batch >= JOURNAL_BATCH_SIZE) {
        _log("Batch limit reached, fsyncing journal");
        POSIX::fsync(fileno($JH));
        $journal_batch = 0;
    }
}

sub journal_flush {
    return unless $JH;
    POSIX::fsync(fileno($JH));
    $journal_batch = 0;
}

sub apply_journal_record {
    my ($r) = @_;
    my $op = $r->{op};

    eval {
        if ($op eq 'create') {
            my $real = fs_realpath($r->{path});
            return unless defined $real;
            return if -e $real; # Idempotent: already exists
            
            my $parent = fs_realpath(parentdir($r->{path}));
            make_path($parent) if defined $parent && !-d $parent;
            
            sysopen(my $fh, $real, O_CREAT|O_RDWR, $r->{mode} & 0777) 
                or die "create: $!";
            POSIX::fsync(fileno($fh));
            close $fh;
            _log("replay: created $r->{path}");
        }
        elsif ($op eq 'write') {
            my $real = fs_realpath($r->{path});
            return unless defined $real && -e $real;
            
            open my $fh, '+<', $real or die "write open: $!";
            binmode $fh;
            seek $fh, $r->{off}, 0;
            my $w = syswrite($fh, $r->{data});
            die "write failed" unless defined $w;
            POSIX::fsync(fileno($fh));
            close $fh;
            _log("replay: wrote " . length($r->{data}) . " bytes to $r->{path}");
        }
        elsif ($op eq 'truncate') {
            my $real = fs_realpath($r->{path});
            return unless defined $real && -e $real;
            
            open my $fh, '+<', $real or die "truncate open: $!";
            truncate($fh, $r->{len}) or die "truncate: $!";
            POSIX::fsync(fileno($fh));
            close $fh;
            _log("replay: truncated $r->{path} to $r->{len}");
        }
        elsif ($op eq 'unlink') {
            my $real = fs_realpath($r->{path});
            return unless defined $real;
            unlink($real) if -e $real; # Idempotent
            _log("replay: unlinked $r->{path}");
        }
        elsif ($op eq 'mkdir') {
            my $real = fs_realpath($r->{path});
            return unless defined $real;
            return if -d $real; # Idempotent: already exists
            
            my $parent = fs_realpath(parentdir($r->{path}));
            make_path($parent) if defined $parent && !-d $parent;
            
            make_path($real, { mode => $r->{mode} & 0777 });
            _log("replay: mkdir $r->{path}");
        }
        elsif ($op eq 'rmdir') {
            my $real = fs_realpath($r->{path});
            return unless defined $real;
            rmdir($real) if -d $real; # Idempotent
            _log("replay: rmdir $r->{path}");
        }
        elsif ($op eq 'rename') {
            my $old_real = fs_realpath($r->{old});
            my $new_real = fs_realpath($r->{new});
            return unless defined $old_real && defined $new_real;
            return unless -e $old_real; # Source must exist
            
            my $parent = fs_realpath(parentdir($r->{new}));
            make_path($parent) if defined $parent && !-d $parent;
            
            rename($old_real, $new_real) or die "rename: $!";
            _log("replay: renamed $r->{old} -> $r->{new}");
        }
        elsif ($op eq 'chmod') {
            my $real = fs_realpath($r->{path});
            return unless defined $real && -e $real;
            chmod($r->{mode} & 0777, $real);
            _log("replay: chmod $r->{path}");
        }
        elsif ($op eq 'utime') {
            my $real = fs_realpath($r->{path});
            return unless defined $real && -e $real;
            utime($r->{atime}, $r->{mtime}, $real);
            _log("replay: utime $r->{path}");
        }
        else {
            _error("Unknown journal operation: $op");
        }
    };
    
    if ($@) {
        _error("replay $op failed: $@");
    }
}

sub replay_journal {
    return unless -e $JOURNAL;

    _log("Replaying journal from $JOURNAL");
    journal_lock();
    
    open my $fh, '<', $JOURNAL or die "cannot read journal: $!";
    binmode $fh;

    my $record_count = 0;
    my $error_count = 0;

    while (1) {
        my $header = <$fh>;
        last unless defined $header;
        chomp $header;
        
        unless ($header =~ /^(\d+)\s+(\d+)$/) {
            _error("Invalid journal header: $header");
            last;
        }

        my ($len, $expected_crc) = ($1, $2);
        my $json = '';
        my $r = read($fh, $json, $len);
        
        unless (defined $r && $r == $len) {
            _error("Truncated journal record (expected $len, got " . ($r // 0) . ")");
            last;
        }

        # Verify CRC
        my $actual_crc = crc32($json);
        if ($actual_crc != $expected_crc) {
            _error("CRC mismatch (expected $expected_crc, got $actual_crc)");
            $error_count++;
            last; # Stop on corruption
        }

        # Decode and apply
        my $rec = eval { decode_json($json) };
        if ($@) {
            _error("JSON decode failed: $@");
            $error_count++;
            last;
        }

        # Validate record structure
        unless (ref($rec) eq 'HASH' && exists $rec->{op}) {
            _error("Invalid journal record structure");
            $error_count++;
            last;
        }

        apply_journal_record($rec);
        $record_count++;
    }

    close $fh;
    journal_unlock();

    _log("Journal replay complete: $record_count records, $error_count errors");

    if ($error_count == 0) {
        # Only truncate journal if replay was successful
        _log("Truncating journal after successful replay");
        open my $wipe, '>', $JOURNAL or _error("Cannot truncate journal: $!");
        close $wipe if $wipe;
    } else {
        _error("Journal replay had errors, keeping journal intact");
    }
}

# ============================================================
# FUSE callbacks (15+ operations)
# ============================================================

sub getattr {
    my ($path) = @_;
    my $real = fs_realpath($path);
    
    unless (defined $real) {
        _log("getattr($path) -> path escape or invalid");
        return -EACCES();
    }
    
    unless (-e $real) {
        _log("getattr($path) -> ENOENT");
        return -ENOENT();
    }
    
    my @st = lstat($real);
    _log("getattr($path) -> mode=$st[2] size=$st[7]");
    return @st;
}

sub readdir {
    my ($path) = @_;
    my $real = fs_realpath($path);
    
    unless (defined $real) {
        _log("readdir($path) -> path escape");
        return -EACCES();
    }
    
    unless (-d $real) {
        _log("readdir($path) -> ENOENT");
        return -ENOENT();
    }

    opendir(my $dh, $real) or do {
        _log("readdir($path) -> EACCES (opendir failed)");
        return -EACCES();
    };
    
    my @entries = readdir($dh);
    closedir($dh);
    
    _log("readdir($path) -> " . scalar(@entries) . " entries");
    return @entries;
}

sub open {
    my ($path, $flags) = @_;
    my $real = fs_realpath($path);
    
    unless (defined $real) {
        _log("open($path) -> path escape");
        return -EACCES();
    }
    
    unless (-e $real && -f $real) {
        _log("open($path) -> ENOENT");
        return -ENOENT();
    }

    my $acc = $flags & O_ACCMODE;
    unless ($acc == O_RDONLY || $acc == O_WRONLY || $acc == O_RDWR) {
        _log("open($path) -> invalid access mode");
        return -EACCES();
    }
    
    _log("open($path, flags=$flags) -> OK");
    return 0;
}

sub read {
    my ($path, $size, $off) = @_;
    
    unless (isfile($path)) {
        _log("read($path) -> ENOENT");
        return -ENOENT();
    }

    my $real = fs_realpath($path);
    open my $fh, '<', $real or do {
        _log("read($path) -> EACCES");
        return -EACCES();
    };
    
    binmode $fh;
    seek $fh, $off, 0;
    my $buf = '';
    my $r = read($fh, $buf, $size);
    close $fh;
    
    unless (defined $r) {
        _log("read($path, size=$size, off=$off) -> read failed");
        return '';
    }
    
    _log("read($path, size=$size, off=$off) -> $r bytes");
    return $buf;
}

sub write {
    my ($path, $buf, $off) = @_;
    
    unless (isfile($path)) {
        _log("write($path) -> ENOENT");
        return -ENOENT();
    }

    journal_record({
        op   => 'write',
        path => $path,
        off  => $off,
        data => $buf,
    });

    my $real = fs_realpath($path);
    open my $fh, '+<', $real or do {
        _log("write($path) -> EACCES");
        return -EACCES();
    };
    
    binmode $fh;
    seek $fh, $off, 0;
    my $w = syswrite($fh, $buf);
    
    unless (defined $w) {
        close $fh;
        _log("write($path) -> syswrite failed");
        return -EACCES();
    }
    
    POSIX::fsync(fileno($fh));
    close $fh;
    
    _log("write($path, size=" . length($buf) . ", off=$off) -> $w bytes");
    return $w;
}

sub create {
    my ($path, $flags, $mode) = @_;

    my $real = fs_realpath($path);
    unless (defined $real) {
        _log("create($path) -> path escape");
        return -EACCES();
    }

    if (-e $real) {
        _log("create($path) -> EEXIST");
        return -EEXIST();
    }

    my $parent = fs_realpath(parentdir($path));
    unless (defined $parent && -d $parent) {
        _log("create($path) -> ENOENT (parent missing)");
        return -ENOENT();
    }

    journal_record({ op => 'create', path => $path, mode => $mode & 0777 });

    sysopen(my $fh, $real, O_CREAT|O_RDWR, $mode & 0777) or do {
        _log("create($path) -> sysopen failed: $!");
        return -EACCES();
    };
    
    POSIX::fsync(fileno($fh));
    close $fh;
    
    _log("create($path, mode=" . sprintf("%04o", $mode & 0777) . ") -> OK");
    return 0;
}

sub truncate {
    my ($path, $len) = @_;
    
    unless (isfile($path)) {
        _log("truncate($path) -> ENOENT");
        return -ENOENT();
    }

    journal_record({ op => 'truncate', path => $path, len => $len });

    my $real = fs_realpath($path);
    open my $fh, '+<', $real or do {
        _log("truncate($path) -> open failed");
        return -EACCES();
    };
    
    unless (truncate($fh, $len)) {
        close $fh;
        _log("truncate($path, $len) -> truncate failed");
        return -EACCES();
    }
    
    POSIX::fsync(fileno($fh));
    close $fh;
    
    _log("truncate($path, $len) -> OK");
    return 0;
}

sub unlink {
    my ($path) = @_;
    
    unless (isfile($path)) {
        _log("unlink($path) -> ENOENT");
        return -ENOENT();
    }

    journal_record({ op => 'unlink', path => $path });

    my $real = fs_realpath($path);
    unless (unlink($real)) {
        _log("unlink($path) -> failed: $!");
        return -EACCES();
    }
    
    _log("unlink($path) -> OK");
    return 0;
}

sub mkdir {
    my ($path, $mode) = @_;

    my $real = fs_realpath($path);
    unless (defined $real) {
        _log("mkdir($path) -> path escape");
        return -EACCES();
    }
    
    if (-e $real) {
        _log("mkdir($path) -> EEXIST");
        return -EEXIST();
    }

    my $parent = fs_realpath(parentdir($path));
    unless (defined $parent && -d $parent) {
        _log("mkdir($path) -> ENOENT (parent missing)");
        return -ENOENT();
    }

    journal_record({ op => 'mkdir', path => $path, mode => $mode & 0777 });

    eval { make_path($real, { mode => $mode & 0777 }); };
    if ($@) {
        _log("mkdir($path) -> failed: $@");
        return -EACCES();
    }
    
    _log("mkdir($path, mode=" . sprintf("%04o", $mode & 0777) . ") -> OK");
    return 0;
}

sub rmdir {
    my ($path) = @_;

    my $real = fs_realpath($path);
    unless (defined $real) {
        _log("rmdir($path) -> path escape");
        return -EACCES();
    }
    
    unless (-d $real) {
        _log("rmdir($path) -> ENOENT");
        return -ENOENT();
    }

    opendir(my $dh, $real) or do {
        _log("rmdir($path) -> opendir failed");
        return -EACCES();
    };
    
    my @ents = grep { $_ ne '.' && $_ ne '..' } readdir($dh);
    closedir($dh);
    
    if (@ents) {
        _log("rmdir($path) -> ENOTEMPTY");
        return -ENOTEMPTY();
    }

    journal_record({ op => 'rmdir', path => $path });

    unless (rmdir($real)) {
        _log("rmdir($path) -> failed: $!");
        return -EACCES();
    }
    
    _log("rmdir($path) -> OK");
    return 0;
}

sub rename {
    my ($old_path, $new_path) = @_;

    my $old_real = fs_realpath($old_path);
    my $new_real = fs_realpath($new_path);
    
    unless (defined $old_real && defined $new_real) {
        _log("rename($old_path -> $new_path) -> path escape");
        return -EACCES();
    }
    
    unless (-e $old_real) {
        _log("rename($old_path -> $new_path) -> ENOENT (source)");
        return -ENOENT();
    }

    my $parent = fs_realpath(parentdir($new_path));
    unless (defined $parent && -d $parent) {
        _log("rename($old_path -> $new_path) -> ENOENT (dest parent)");
        return -ENOENT();
    }

    journal_record({ op => 'rename', old => $old_path, new => $new_path });

    unless (rename($old_real, $new_real)) {
        _log("rename($old_path -> $new_path) -> failed: $!");
        return -EACCES();
    }
    
    _log("rename($old_path -> $new_path) -> OK");
    return 0;
}

sub chmod {
    my ($path, $mode) = @_;

    my $real = fs_realpath($path);
    unless (defined $real) {
        _log("chmod($path) -> path escape");
        return -EACCES();
    }
    
    unless (-e $real) {
        _log("chmod($path) -> ENOENT");
        return -ENOENT();
    }

    journal_record({ op => 'chmod', path => $path, mode => $mode & 0777 });

    unless (chmod($mode, $real)) {
        _log("chmod($path, mode=" . sprintf("%04o", $mode & 0777) . ") -> failed");
        return -EACCES();
    }
    
    _log("chmod($path, mode=" . sprintf("%04o", $mode & 0777) . ") -> OK");
    return 0;
}

sub utime {
    my ($path, $atime, $mtime) = @_;

    my $real = fs_realpath($path);
    unless (defined $real) {
        _log("utime($path) -> path escape");
        return -EACCES();
    }
    
    unless (-e $real) {
        _log("utime($path) -> ENOENT");
        return -ENOENT();
    }

    journal_record({ op => 'utime', path => $path, atime => $atime, mtime => $mtime });

    unless (utime($atime, $mtime, $real)) {
        _log("utime($path) -> failed");
        return -EACCES();
    }
    
    _log("utime($path, atime=$atime, mtime=$mtime) -> OK");
    return 0;
}

sub chown {
    # Not implemented; let kernel know
    return -ENOSYS();
}

sub statfs {
    # Return filesystem statistics
    my $bsize  = 4096;
    my $blocks = 1_000_000;
    my $bfree  = 900_000;
    my $files  = 1_000_000;
    my $ffree  = 900_000;
    my $namelen = 255;
    return ($bsize, $blocks, $bfree, $files, $ffree, $namelen);
}

sub flush {
    journal_flush();
    return 0;
}

sub release {
    my ($path, $flags) = @_;
    _log("release($path, flags=" . ($flags // 0) . ")");
    return 0;
}

# ============================================================
# Test harness (no FUSE, direct callback calls)
# ============================================================

sub run_test_harness {
    print "\n" . "="x60 . "\n";
    print "PerlFS Test Harness (Direct Callback Testing)\n";
    print "="x60 . "\n\n";
    print "Available commands:\n";
    print "  ls <path>              - List directory\n";
    print "  stat <path>            - Show file stats\n";
    print "  cat <path>             - Read file contents\n";
    print "  echo <text> > <path>   - Write text to file\n";
    print "  touch <path>           - Create empty file\n";
    print "  mkdir <path>           - Create directory\n";
    print "  rmdir <path>           - Remove directory\n";
    print "  rm <path>              - Delete file\n";
    print "  mv <old> <new>         - Rename/move file\n";
    print "  truncate <path> <len>  - Truncate file\n";
    print "  chmod <mode> <path>    - Change permissions (octal)\n";
    print "  journal                - Show journal info\n";
    print "  flush                  - Flush journal to disk\n";
    print "  help                   - Show this help\n";
    print "  quit                   - Exit test harness\n";
    print "\n";

    while (1) {
        print "perlfs> ";
        my $line = <STDIN>;
        last unless defined $line;
        chomp $line;
        next if $line =~ /^\s*$/;

        # Parse command line
        my @tokens = $line =~ /(?:[^\s"']+|"[^"]*"|'[^']*')+/g;
        @tokens = map { s/^["']|["']$//g; $_ } @tokens;
        
        next unless @tokens;
        my $cmd = shift @tokens;

        if ($cmd eq 'quit' || $cmd eq 'exit') {
            print "Exiting test harness.\n";
            last;
        }
        elsif ($cmd eq 'help') {
            run_test_harness(); # Re-display help
            return;
        }
        elsif ($cmd eq 'ls') {
            my $path = $tokens[0] // '/';
            my @ents = readdir($path);
            if (@ents && ref($ents[0]) ne 'SCALAR') {
                print join("  ", sort @ents), "\n";
            } else {
                print "Error: Cannot list directory\n";
            }
        }
        elsif ($cmd eq 'stat') {
            my $path = $tokens[0] // '/';
            my @st = getattr($path);
            if (@st && $st[0] != -ENOENT()) {
                printf "mode=%04o uid=%d gid=%d size=%d\n", 
                    $st[2] & 07777, $st[4], $st[5], $st[7];
            } else {
                print "Error: File not found\n";
            }
        }
        elsif ($cmd eq 'cat') {
            my $path = $tokens[0];
            unless ($path) {
                print "Usage: cat <path>\n";
                next;
            }
            my $content = read($path, 10 * 1024 * 1024, 0); # 10MB max
            if (ref($content) eq 'SCALAR' && $$content < 0) {
                print "Error: Cannot read file\n";
            } else {
                print $content;
                print "\n" unless $content =~ /\n$/;
            }
        }
        elsif ($cmd eq 'echo' && @tokens >= 3 && $tokens[-2] eq '>') {
            my $path = pop @tokens;
            pop @tokens; # Remove '>'
            my $text = join(' ', @tokens);
            
            # Create file if it doesn't exist
            unless (isfile($path)) {
                my $rc = create($path, 0, 0644);
                if ($rc != 0) {
                    print "Error: Cannot create file (rc=$rc)\n";
                    next;
                }
            }
            
            my $rc = write($path, $text . "\n", 0);
            if ($rc > 0) {
                print "Wrote $rc bytes\n";
            } else {
                print "Error: Write failed (rc=$rc)\n";
            }
        }
        elsif ($cmd eq 'touch') {
            my $path = $tokens[0];
            unless ($path) {
                print "Usage: touch <path>\n";
                next;
            }
            my $rc = create($path, 0, 0644);
            print $rc == 0 ? "OK\n" : "Error: rc=$rc\n";
        }
        elsif ($cmd eq 'mkdir') {
            my $path = $tokens[0];
            unless ($path) {
                print "Usage: mkdir <path>\n";
                next;
            }
            my $rc = mkdir($path, 0755);
            print $rc == 0 ? "OK\n" : "Error: rc=$rc\n";
        }
        elsif ($cmd eq 'rmdir') {
            my $path = $tokens[0];
            unless ($path) {
                print "Usage: rmdir <path>\n";
                next;
            }
            my $rc = rmdir($path);
            print $rc == 0 ? "OK\n" : "Error: rc=$rc\n";
        }
        elsif ($cmd eq 'rm') {
            my $path = $tokens[0];
            unless ($path) {
                print "Usage: rm <path>\n";
                next;
            }
            my $rc = unlink($path);
            print $rc == 0 ? "OK\n" : "Error: rc=$rc\n";
        }
        elsif ($cmd eq 'mv') {
            my ($old, $new) = @tokens;
            unless ($old && $new) {
                print "Usage: mv <old> <new>\n";
                next;
            }
            my $rc = rename($old, $new);
            print $rc == 0 ? "OK\n" : "Error: rc=$rc\n";
        }
        elsif ($cmd eq 'truncate') {
            my ($path, $len) = @tokens;
            unless ($path && defined $len) {
                print "Usage: truncate <path> <length>\n";
                next;
            }
            my $rc = truncate($path, $len);
            print $rc == 0 ? "OK\n" : "Error: rc=$rc\n";
        }
        elsif ($cmd eq 'chmod') {
            my ($mode, $path) = @tokens;
            unless ($mode && $path) {
                print "Usage: chmod <mode> <path>\n";
                next;
            }
            my $mode_num = oct($mode);
            my $rc = chmod($path, $mode_num);
            print $rc == 0 ? "OK\n" : "Error: rc=$rc\n";
        }
        elsif ($cmd eq 'journal') {
            if (-e $JOURNAL) {
                my $size = -s $JOURNAL;
                print "Journal: $JOURNAL\n";
                print "Size: $size bytes\n";
                print "Max size: " . MAX_JOURNAL_SIZE . " bytes\n";
            } else {
                print "Journal does not exist\n";
            }
        }
        elsif ($cmd eq 'flush') {
            journal_flush();
            print "Journal flushed\n";
        }
        else {
            print "Unknown command: $cmd (type 'help' for commands)\n";
        }
    }
}

# ============================================================
# Argument parsing / main
# ============================================================

sub parse_args {
    my @argv = @_;
    my $mode = '';   # '', '--test', '--debug'
    my $root;
    my $mount;

    while (@argv) {
        my $arg = shift @argv;
        if ($arg eq '--test') {
            $mode = '--test';
        }
        elsif ($arg eq '--debug') {
            $mode = '--debug';
        }
        elsif ($arg eq '--root') {
            $root = shift @argv or die "Error: --root requires a path\n";
        }
        elsif ($arg eq '--help' || $arg eq '-h') {
            print_usage();
            exit 0;
        }
        elsif (!defined $mount && $arg !~ /^--/) {
            $mount = $arg;
        }
        else {
            die "Error: Unknown argument: $arg\n";
        }
    }

    if ($mode eq '--test') {
        die "Usage: $0 --test --root <backing_root>\n" unless $root;
    }
    elsif ($mode eq '--debug') {
        die "Usage: $0 --debug --root <backing_root> <mountpoint>\n"
            unless $root && $mount;
    }
    else {
        die "Usage: $0 --root <backing_root> <mountpoint>\n"
            unless $root && $mount;
    }

    return ($mode, $root, $mount);
}

sub print_usage {
    print <<"END_USAGE";
PerlFS - A journaling FUSE filesystem in Perl

Usage:
  $0 --root <backing_root> <mountpoint>
  $0 --debug --root <backing_root> <mountpoint>
  $0 --test --root <backing_root>

Options:
  --root <path>      Backing directory for filesystem storage
  --debug            Enable debug logging and mount filesystem
  --test             Run interactive test harness (no FUSE mount)
  --help, -h         Show this help message

Examples:
  # Mount filesystem
  $0 --root /tmp/perlfs_data /mnt/perlfs

  # Mount with debug logging
  $0 --debug --root /tmp/perlfs_data /mnt/perlfs

  # Run test harness
  $0 --test --root /tmp/perlfs_data

END_USAGE
}

# ============================================================
# Main execution
# ============================================================

my ($mode, $root, $mountpoint) = parse_args(@ARGV);

$ROOT = $root;
die "Error: root '$ROOT' is not a directory\n" unless -d $ROOT;

$JOURNAL = File::Spec->catfile($ROOT, '.perlfs_journal');
$JOURNAL_LOCK = File::Spec->catfile($ROOT, '.perlfs_journal.lock');

# Enable debug logging in test/debug modes
$DEBUG = 1 if $mode eq '--debug' || $mode eq '--test';

# Recover from crashes
_log("Starting journal replay");
replay_journal();

# Open journal for writing
journal_open();

if ($mode eq '--test') {
    run_test_harness();
    journal_close();
    exit 0;
}

die "Error: mountpoint '$mountpoint' is not a directory\n" unless -d $mountpoint;

_log("Starting PerlFS");
_log("Root directory: $ROOT");
_log("Mount point: $mountpoint");
_log("Journal: $JOURNAL");

# Mount filesystem
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

# Cleanup on exit
journal_close();
_log("PerlFS stopped");
