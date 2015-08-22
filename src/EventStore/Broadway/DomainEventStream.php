<?php
namespace EventStore\Broadway;

use Broadway\Domain\DomainEventStreamInterface;
use EventStore\StreamFeed\StreamFeedIterator;

final class DomainEventStream implements DomainEventStreamInterface
{
    private $iterator;

    public function __construct($id, StreamFeedIterator $iterator)
    {
        $this->iterator = new DomainEventStreamIterator($id, $iterator);
    }

    public function getIterator()
    {
        return $this->iterator;
    }
}
