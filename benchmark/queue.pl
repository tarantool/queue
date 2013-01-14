
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

use Coro;
use DR::Tarantool ':all';
use DR::Tarantool::StartTest;
use Time::HiRes 'time';
use Data::Dumper;
use Coro::AnyEvent;

my $t = DR::Tarantool::StartTest->run(
    cfg         => catfile(cwd, 'config/db/tarantool.cfg'),
    script_dir  => catfile(cwd, 'config/db')
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

tnt->ping;

my $done = 0;
my $total_time = 0;
my $process = 1;

$SIG{INT} = $SIG{TERM} = sub {
    print "\nПолучен сигнал выхода\n";
    $process = 0;
};


while($process) {

    my $start_time = time;
    my (@f, %t);
    my $no = 0;
    for (my $i = 0; $i < 250; $i++) {
        push @f => async {
            my $tuple = tnt->call_lua('queue.put', [ 0, 'test' ]);
            $t{ $tuple->raw(0) }++ if $tuple;
        };

        push @f => async {
            my $tuple = tnt->call_lua('queue.take', [ 0, 'test', 3 ]);
            $t{ $tuple->raw(0) }++ if $tuple;

            Coro::AnyEvent::sleep .1;
            tnt->call_lua('queue.ack', [ 0, $tuple->raw(0) ]);
        };

        $no++;

    }

    $_->join for @f;
    @f = ();

    my $done_time = time - $start_time;
    $total_time += $done_time;
    $done += $no;


    printf "Done %d sessions in %3.2f seconds (%d r/s, %f s/r)\n",
        $done,
        $total_time,
        $done / $total_time,
        $total_time / $done
    ;

}

warn $t->log;
