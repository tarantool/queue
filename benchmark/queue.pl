
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
    cfg         => catfile(cwd, 'tarantool.cfg'),
    script_dir  => catfile(cwd)
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
    print "\nSIGING received\n";
    $t->kill unless $process;
    $process = 0;
};


while($process) {

    my $start_time = time;
    my (@f, %t);
    my $no = 0;
    for (my $i = 0; $i < 500; $i++) {
        push @f => async {
            my $tuple = tnt->call_lua('queue.put',
                [ 0, 'tube', 0, 10, 5, 1, 'task body' ]);
            $t{ $tuple->raw(0) }++;
        };

        push @f => async {{
            my $tuple = tnt->call_lua('queue.take', [ 0, 'tube', 3 ]);
            redo unless $tuple;
            $t{ $tuple->raw(0) }++ if $tuple;

            tnt->call_lua('queue.ack', [ 0, $tuple->raw(0) ]);
        }};

        $no++;

    }

    $_->join for @f;
    @f = ();

    my $done_time = time - $start_time;
    $total_time += $done_time;
    $done += $no;

    if (scalar keys %t != $no) {
        print "Некорректное число результатов\n";
        last;
    }
    if ($no != grep { $_ == 2 } values %t) {
        print "Не все результаты фигурируют два раза\n";
        last;
    }

    printf "Done %d sessions in %3.2f seconds (%d r/s, %f s/r)\n",
        $done,
        $total_time,
        $done / $total_time,
        $total_time / $done
    ;

}

warn $t->log if $ENV{DEBUG};
