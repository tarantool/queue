package DR::TarantoolQueue;
use DR::Tarantool ();
use utf8;
use strict;
use warnings;
use Mouse;
use Carp;
use JSON::XS;
require DR::TarantoolQueue::Task;

our $VERSION = '0.02';

=head1 NAME

DR::TarantoolQueue - client for tarantool's queue


=head1 SYNOPSIS

    my $queue = DR::TarantoolQueue->new(
        host    => 'tarantool.host',
        port    => 33014,
        tube    => 'request_queue',
        space   => 11
    );


    # put empty task into queue with name 'request_queue'
    my $task = $queue->put;

    my $task = $queue->put(data => [ 1, 2, 3 ]);

    printf "task.id = %s\n", $task->id;
=cut

has host    => (is => 'ro', isa => 'Str',   required => 1);
has port    => (is => 'ro', isa => 'Str',   required => 1);
has coro    => (is => 'ro', isa => 'Bool',  default  => 1);

has ttl     => (is => 'rw', isa => 'Num|Undef');
has ttr     => (is => 'rw', isa => 'Num|Undef');
has space   => (is => 'rw', isa => 'Str|Undef');
has tube    => (is => 'rw', isa => 'Str|Undef');
with 'DR::TarantoolQueue::JSE';


sub tnt {
    my ($self) = @_;

    unless ($self->coro) {
        return $self->{tnt} if $self->{tnt};
        $self->{tnt} = DR::Tarantool::tarantool
            port => $self->port,
            host => $self->host,
            spaces => {}
        ;
    }

    require Coro;
    return $self->{tnt} if $self->{tnt};
    if ($self->{tnt_waiter}) {
        push @{ $self->{tnt_waiter} } => $Coro::current;
        Coro::schedule;
        return $self->{tnt};
    }
    $self->{tnt_waiter} = [];
    $self->{tnt} = DR::Tarantool::coro_tarantool
        port => $self->port,
        host => $self->host,
        spaces => {}
    ;
    $_->ready for @{ $self->{tnt_waiter} };
    delete $self->{tnt_waiter};
    return $self->{tnt};
}


sub _check_opts($@) {
    my $h = shift;
    my %can = map { ($_ => 1) } @_;

    for (keys %$h) {
        next if $can{$_};
        croak 'unknown option: ' . $_;
    }
}

sub _producer {
    my ($self, $method, $o) = @_;

    _check_opts $o, qw(space tube delay ttl ttr pri data);

    $o->{space} = $self->space unless defined $o->{space};
    croak 'space was not defined' unless defined $o->{space};

    $o->{tube}  = $self->tube unless defined $o->{tube};
    croak 'tube was not defined' unless defined $o->{tube};

    $o->{ttl} ||= $self->ttl || 0;
    $o->{ttr} ||= $self->ttr || 0;
    $o->{delay} ||= 0;
    $o->{pri} ||= 0;

    

    my $tuple = $self->tnt->call_lua(
        "queue.$method" => [
            $o->{space},
            $o->{tube},
            $o->{delay},
            $o->{ttl},
            $o->{ttr},
            $o->{pri},
            $self->jse->encode($o->{data})
        ]
    );

    return DR::TarantoolQueue::Task->new(
        id      => $tuple->raw(0),
        rawdata => $tuple->raw(1),
        space   => $o->{space},
        tube    => $o->{tube},
        queue   => $self,
    );
}

sub put {
    my ($self, %opts) = @_;
    return $self->_producer(put => \%opts);
}

sub urgent {
    my ($self, %opts) = @_;
    return $self->_producer(urgent => \%opts);
}

sub take {
    my ($self, %o) = @_;
    _check_opts \%o, qw(space tube timeout);
    $o{space} = $self->space unless defined $o{space};
    croak 'space was not defined' unless defined $o{space};
    $o{tube} = $self->tube unless defined $o{tube};
    croak 'tube was not defined' unless defined $o{tube};
    $o{timeout} ||= 0;
    
    my $tuple = $self->tnt->call_lua(
        'queue.take' => [
            $o{space},
            $o{tube},
            $o{timeout}
        ]
    );
    return unless $tuple;
    return DR::TarantoolQueue::Task->new(
        id      => $tuple->raw(0),
        rawdata => $tuple->raw(1),
        space   => $o{space},
        tube    => $o{tube},
        queue   => $self,
    );
}

sub ack {
    my ($self, %o) = @_;
    _check_opts \%o, qw(task);
    croak 'task was not defined' unless $o{task};
    my $tuple = $self->tnt->call_lua(
        'queue.ack' => [
            $o{task}->space,
            $o{task}->id,
        ]
    );
    return DR::TarantoolQueue::Task->new(
        id      => $tuple->raw(0),
        rawdata => $tuple->raw(1),
        space   => $o{task}->space,
        tube    => $o{task}->tube,
        queue   => $self,
    );
}


__PACKAGE__->meta->make_immutable();
