package DR::TarantoolQueue::Task;
use utf8;
use strict;
use warnings;
use Mouse;
use JSON::XS ();

has id      => (is => 'ro', isa => 'Str', required => 1);
has rawdata => (is => 'ro', isa => 'Str', required => 1);
has data    => (
    is          => 'ro',
    isa         => 'HashRef|ArrayRef|Undef',
    lazy        => 1,
    builder     => '_build_data'
);

with 'DR::TarantoolQueue::JSE';


sub _build_data {
    my ($self) = @_;
    $self->jse->decode( $self->rawdata );
}

__PACKAGE__->meta->make_immutable();
