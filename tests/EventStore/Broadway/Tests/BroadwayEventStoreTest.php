<?php
namespace EventStore\Broadway\Tests;

use Broadway\Domain\DateTime;
use Broadway\Domain\DomainEventStream;
use Broadway\Domain\DomainMessage;
use Broadway\Domain\Metadata;
use Broadway\Serializer\SerializableInterface;
use EventStore\Broadway\BroadwayEventStore;
use EventStore\EventStore;
use ValueObjects\Identity\UUID;

class BroadwayEventStoreTest extends \PHPUnit_Framework_TestCase
{
    private $eventStore;

    protected function setUp()
    {
        $this->eventStore = new BroadwayEventStore(
            new EventStore('http://127.0.0.1:2113')
        );
    }

    /**
     * @test
     */
    public function it_should_create_a_new_entry_when_id_is_new()
    {
        $id = (string) new UUID;
        $domainEventStream = new DomainEventStream([
            $this->createDomainMessage($id, 0),
            $this->createDomainMessage($id, 1),
            $this->createDomainMessage($id, 2),
            $this->createDomainMessage($id, 3),
        ]);

        $this->eventStore->append($id, $domainEventStream);

        $this->assertEquals(
            iterator_to_array($domainEventStream),
            iterator_to_array($this->eventStore->load($id))
        );
    }

    /**
     * @test
     */
    public function it_should_append_to_an_already_existing_stream()
    {
        $id = (string) new UUID;
        $dateTime = DateTime::fromString('2014-03-12T14:17:19.176169+00:00');
        $domainEventStream = new DomainEventStream(array(
            $this->createDomainMessage($id, 0, $dateTime),
            $this->createDomainMessage($id, 1, $dateTime),
            $this->createDomainMessage($id, 2, $dateTime),
        ));
        $this->eventStore->append($id, $domainEventStream);
        $appendedEventStream = new DomainEventStream(array(
            $this->createDomainMessage($id, 3, $dateTime),
            $this->createDomainMessage($id, 4, $dateTime),
            $this->createDomainMessage($id, 5, $dateTime),

        ));

        $this->eventStore->append($id, $appendedEventStream);

        $expected = new DomainEventStream(array(
            $this->createDomainMessage($id, 0, $dateTime),
            $this->createDomainMessage($id, 1, $dateTime),
            $this->createDomainMessage($id, 2, $dateTime),
            $this->createDomainMessage($id, 3, $dateTime),
            $this->createDomainMessage($id, 4, $dateTime),
            $this->createDomainMessage($id, 5, $dateTime),
        ));

        $this->assertEquals(
            iterator_to_array($expected),
            iterator_to_array($this->eventStore->load($id))
        );

    }

    /**
     * @test
     */
    public function streams_are_loaded_in_the_right_order()
    {
        $id = (string) new UUID;
        $dateTime = DateTime::fromString('2014-03-12T14:17:19.176169+00:00');

        $events = [];
        // the eventstore paginates results with 20 events per page
        for ($i = 0; $i <= 22; $i++) {
            $events[] = $this->createDomainMessage($id, $i, $dateTime, 'event-' . $i);
        }

        $events = new DomainEventStream($events);
        $this->eventStore->append($id, $events);

        $events = $this->eventStore->load($id);
        $eventIterator = $events->getIterator();
        $firstMessage = $eventIterator->current();

        $this->assertEquals(23, iterator_count($eventIterator));
        $this->assertEquals('event-0', $firstMessage->getPayload()->title);
    }

    /**
     * @test
     */
    public function metadata_is_saved_properly()
    {
        $id = (string) new UUID;
        $dateTime = DateTime::fromString('2014-03-12T14:17:19.176169+00:00');
        $metadata = ['foo' => 'bar'];

        $events = [$this->createDomainMessageWithMetadata($id, 0, $metadata, $dateTime, 'event-42')];

        $events = new DomainEventStream($events);
        $this->eventStore->append($id, $events);

        $events = $this->eventStore->load($id);
        $eventIterator = $events->getIterator();
        $firstMessage = $eventIterator->current();

        $this->assertEquals($metadata, $firstMessage->getMetadata()->serialize());
    }

    /**
     * @test
     * @expectedException Broadway\EventStore\EventStreamNotFoundException
     */
    public function it_should_throw_an_exception_when_requesting_the_stream_of_a_non_existing_aggregate()
    {
        $id = (string) new UUID;
        $this->eventStore->load($id);
    }

    /**
     * @test
     * @expectedException Broadway\EventStore\EventStoreException
     */
    public function it_should_throw_an_exception_when_appending_a_duplicate_playhead()
    {
        $id                = (string) new UUID;
        $domainMessage     = $this->createDomainMessage($id, 0);
        $baseStream        = new DomainEventStream(array($domainMessage));
        $this->eventStore->append($id, $baseStream);
        $appendedEventStream = new DomainEventStream(array($domainMessage));

        $this->eventStore->append($id, $appendedEventStream);
    }

    private function createDomainMessage($id, $playhead, $recordedOn = null, $title = '')
    {
        return new DomainMessage($id, $playhead, new Metadata([]), new Event($title), $recordedOn ? $recordedOn : DateTime::now());
    }

    private function createDomainMessageWithMetadata($id, $playhead, array $metadata, $recordedOn = null, $title = '')
    {
        return new DomainMessage($id, $playhead, new Metadata($metadata), new Event($title), $recordedOn ? $recordedOn : DateTime::now());
    }
}

class Event implements SerializableInterface
{

    /**
     * @var string
     */
    public $title;

    /**
     * @param string $title
     */
    public function __construct($title)
    {
        $this->title = $title;
    }

    public static function deserialize(array $data)
    {
        return new Event($data['title']);
    }

    public function serialize()
    {
        return [
            'title' => $this->title
        ];
    }
}
