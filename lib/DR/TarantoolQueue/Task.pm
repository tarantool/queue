package DR::TarantoolQueue::Task;
use utf8;
use strict;
use warnings;
use Mouse;
use JSON::XS ();
use Carp;

has space   => (is => 'ro', isa => 'Str', required => 1);
has status  => (is => 'ro', isa => 'Str', required => 1);
has tube    => (is => 'ro', isa => 'Str', required => 1);
has id      => (is => 'ro', isa => 'Str', required => 1);
has rawdata => (is => 'ro', isa => 'Str', required => 1);
has data    => (
    is          => 'ro',
    isa         => 'HashRef|ArrayRef|Undef',
    lazy        => 1,
    builder     => '_build_data'
);

has queue   => (is => 'ro', isa => 'Object|Undef', weak_ref => 1);

with 'DR::TarantoolQueue::JSE';


sub _build_data {
    my ($self) = @_;
    $self->jse->decode( $self->rawdata );
}


for my $m (qw(ack requeue bury dig unbury delete get_meta peek)) {
    no strict 'refs';
    next if *{ __PACKAGE__ . "::$m" }{CODE};
    *{ __PACKAGE__ . "::$m" } = sub {
        my ($self) = @_;
        croak "Can't find queue for task" unless $self->queue;
        return $self->queue->$m(task => $self);
    }
}


sub tuple {
    my ($class, $tuple, $space, $queue) = @_;
    return undef unless $tuple;
    my $raw = $tuple->raw;
    return $class->new(
        id      => $raw->[0],
        tube    => $raw->[1],
        status  => $raw->[2],
        rawdata => $raw->[3],
        space   => $space,
        queue   => $queue,
    );
}




__PACKAGE__->meta->make_immutable();
