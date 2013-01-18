#!/usr/bin/perl

use warnings;
use strict;
use utf8;
use open qw(:std :utf8);
use lib qw(lib ../lib);

use Encode qw(decode encode);
use Cwd 'cwd';
use File::Spec::Functions 'catfile';
use feature 'state';

# use Coro;
use DR::Tarantool ':all';
use DR::Tarantool::StartTest;
use Time::HiRes 'time';
use Data::Dumper;

my $t = DR::Tarantool::StartTest->run(
    cfg         => catfile(cwd, 'tarantool.cfg'),
    script_dir  => catfile(cwd, 'benchmark')
);

sub tnt {
    our $tnt;
    unless(defined $tnt) {
        $tnt = coro_tarantool
            host => 'localhost',
            port => $t->primary_port,
            spaces => {}
        ;
    }
    return $tnt;
};

$| = 1;

my $process = 1;
$SIG{INT} = $SIG{TERM} = sub { print "\ncaught SIGexit\n"; $process = 0 };

my $done = 0;
my $total_time = 0;
while($process) {
    my $started = time;
    for (1 .. 10000) {
        die "Can't ping tarantool" unless tnt->call_lua('ping', [1,2,3]);
    }

    $done += 10000;
    my $done_time = time - $started;
    $total_time += $done_time;
    printf "Done %d pings in %3.2f second (%f s/p and %d p/s)\n",
        $done,
        $done_time,
        $total_time  / $done,
        $done / $total_time
    ;
}
