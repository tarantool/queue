package DR::TarantoolQueue;
use DR::Tarantool ();
use utf8;
use strict;
use warnings;
use Mouse;
use Carp;

=head1 NAME

DR::TarantoolQueue - client for tarantool's queue


=head1 SYNOPSIS

    my $queue = DR::TarantoolQueue->new(
        host    => 'tarantool.host',
        port    => 33014,
        name    => 'request_queue',
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
has name    => (is => 'rw', isa => 'Str|Undef');


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

sub _producer {
    my ($self, $method, $o) = @_;

    $o->{space} = $self->space unless defined $o->{space};
    croak 'space was not defined' unless defined $o->{space};

    $o->{name}  = $self->name unless defined $o->{name};

    $o->{ttl} ||= $self->ttl || 0;
    $o->{ttr} ||= $self->ttr || 0;

}

sub put {
    my ($self, %opts) = @_;
    return $self->_producer(put => \%opts);
}

sub urgent {
    my ($self, %opts) = @_;
    return $self->_producer(urgent => \%opts);
}

__PACKAGE__->meta->make_immutable();
