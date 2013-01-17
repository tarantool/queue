package DR::TarantoolQueue::JSE;
use Mouse::Role;
use utf8;
use strict;
use warnings;


has jse => (
    is      => 'ro',
    isa     => 'Object',
    lazy    => 1,
    builder => '_build_jse'
);

sub _build_jse {
    return JSON::XS->new->allow_nonref->utf8;
}

1;
