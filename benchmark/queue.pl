
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

my (@f, %t);
my $no = 1;
for (my $i = 0; $i < 25; $i++) {
    push @f => async {
        eval {
            my $tuple = tnt->call_lua('queue.put', [ 0, 'test' ]);
            $t{ $tuple->raw(0) }++;
            warn "@{[ $no++ ]} put task";
            1;
        } or warn $@;
    };

    push @f => async {
        eval {
            my $tuple = tnt->call_lua('queue.take', [ 0, 'test' ]);
            $t{ $tuple->raw(0) }++;
            warn "@{[ $no++ ]} take task";
            1;
        } or warn $@;
    };

}

$_->join for @f;
# print $t->log;
