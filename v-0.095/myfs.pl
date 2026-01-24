#!/usr/bin/env perl
use strict;
use warnings;
use Fuse;
use POSIX qw(ENOENT ENOSYS EACCES);
use Fcntl qw(S_IFDIR S_IFREG O_RDONLY O_WRONLY O_RDWR);
use constant O_ACCMODE => 3;  # Mask for file access modes
use Time::HiRes qw(time);

# Simple stderr logger
sub _log { warn "[myfs] $_[0]\n"; }

# Effective GID
my ($EGID) = (split ' ', $))[0];

# In-memory filesystem data
my %files = (
    '/hello.txt' => "Hello from Perl FUSE!\n",
    '/readme.txt' => "This is a simple FUSE filesystem implemented in Perl.\n",
    '/info.txt' => "You can read files but not write to them.\n",
);

sub getattr {
    my $path = shift;
    _log("getattr($path)");

    # Root directory
    if ($path eq '/') {
        my $nlink = 2 + scalar(keys %files);  # . + .. + subdirs
        return (
            0,              # dev
            1,              # ino
            S_IFDIR | 0755, # mode
            $nlink,         # nlink
            $<,             # uid
            $EGID,          # gid
            0,              # rdev
            4096,           # size
            time,           # atime
            time,           # mtime
            time,           # ctime
            4096,           # blksize
            1               # blocks
        );
    }

    # Check if file exists
    if (exists $files{$path}) {
        my $size = length($files{$path});
        my $blocks = int(($size + 511) / 512);
        return (
            0,              # dev
            2,              # ino
            S_IFREG | 0644, # mode
            1,              # nlink
            $<,             # uid
            $EGID,          # gid
            0,              # rdev
            $size,          # size
            time,           # atime
            time,           # mtime
            time,           # ctime
            4096,           # blksize
            $blocks         # blocks
        );
    }

    return -ENOENT();
}

sub readdir {
    my ($path, $offset) = @_;
    _log("readdir($path, offset=" . ($offset // 0) . ")");
    
    if ($path eq '/') {
        my @entries = ('.', '..', map { substr($_, 1) } keys %files);
        return (@entries, 0);
    }
    
    return -ENOENT();
}

sub open {
    my ($path, $flags) = @_;
    _log("open($path, flags=$flags)");
    
    return -ENOENT() unless exists $files{$path};
    
    # Only allow read-only access
    my $access_mode = $flags & O_ACCMODE;
    if ($access_mode != O_RDONLY) {
        return -EACCES();
    }
    
    return 0;
}

sub read {
    my ($path, $size, $offset) = @_;
    _log("read($path, size=$size, offset=$offset)");
    
    return -ENOENT() unless exists $files{$path};
    
    my $content = $files{$path};
    my $len = length($content);
    
    return '' if $offset >= $len;  # EOF
    
    my $bytes_to_read = $size;
    if ($offset + $size > $len) {
        $bytes_to_read = $len - $offset;
    }
    
    my $data = substr($content, $offset, $bytes_to_read);
    return $data;
}

sub statfs {
    _log("statfs()");
    # Return filesystem statistics
    # (namelen, files, files_free, blocks, blocks_avail, blocksize)
    return (255, scalar(keys %files), 1000, 1000, 1000, 4096);
}

sub flush {
    my $path = shift;
    _log("flush($path)");
    return 0;
}

sub release {
    my ($path, $flags) = @_;
    _log("release($path, flags=" . ($flags // 0) . ")");
    return 0;
}

# Stubs for write operations (read-only filesystem)
sub write {
    my $path = shift;
    _log("write($path) -> ENOSYS");
    return -ENOSYS();
}

sub mkdir {
    my $path = shift;
    _log("mkdir($path) -> ENOSYS");
    return -ENOSYS();
}

sub unlink {
    my $path = shift;
    _log("unlink($path) -> ENOSYS");
    return -ENOSYS();
}

sub rmdir {
    my $path = shift;
    _log("rmdir($path) -> ENOSYS");
    return -ENOSYS();
}

sub rename {
    my ($old, $new) = @_;
    _log("rename($old -> $new) -> ENOSYS");
    return -ENOSYS();
}

sub create {
    my $path = shift;
    _log("create($path) -> ENOSYS");
    return -ENOSYS();
}

sub truncate {
    my ($path, $len) = @_;
    _log("truncate($path, $len) -> ENOSYS");
    return -ENOSYS();
}

sub utime {
    my ($path, $atime, $mtime) = @_;
    _log("utime($path) -> ENOSYS");
    return -ENOSYS();
}

sub chmod {
    my ($path, $mode) = @_;
    _log("chmod($path) -> ENOSYS");
    return -ENOSYS();
}

sub chown {
    my ($path, $uid, $gid) = @_;
    _log("chown($path) -> ENOSYS");
    return -ENOSYS();
}

# Get mountpoint from command line
my $mountpoint = shift(@ARGV) or die "Usage: $0 <mountpoint>\n";

# Check if mountpoint exists
unless (-d $mountpoint) {
    die "Error: Mount point '$mountpoint' does not exist or is not a directory.\n";
}

_log("Starting FUSE filesystem on $mountpoint");
_log("Files available: " . join(", ", keys %files));

# Mount the filesystem using hash-style interface (more modern)
Fuse::main(
    mountpoint => $mountpoint,
    getattr    => \&getattr,
    readdir    => \&readdir,
    open       => \&open,
    read       => \&read,
    write      => \&write,
    statfs     => \&statfs,
    flush      => \&flush,
    release    => \&release,
    mkdir      => \&mkdir,
    unlink     => \&unlink,
    rmdir      => \&rmdir,
    rename     => \&rename,
    create     => \&create,
    truncate   => \&truncate,
    utime      => \&utime,
    chmod      => \&chmod,
    chown      => \&chown,
    threaded   => 0,
    debug      => 0,
);
