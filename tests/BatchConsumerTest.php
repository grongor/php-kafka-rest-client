<?php

declare(strict_types=1);

namespace Grongor\KafkaRest\Tests;

use Assert\AssertionFailedException;
use DateInterval;
use DateTimeImmutable;
use Exception;
use Generator;
use Grongor\KafkaRest\Api\RestClient;
use Grongor\KafkaRest\Api\Value\Request\Offset;
use Grongor\KafkaRest\Api\Value\Response\Consumer;
use Grongor\KafkaRest\Api\Value\Response\Message;
use Grongor\KafkaRest\BatchConsumer;
use Grongor\KafkaRest\Exception\ConsumerClosed;
use Grongor\KafkaRest\Value\MessagesBatch;
use Hamcrest\Core\IsEqual;
use Lcobucci\Clock\FrozenClock;
use Mockery;
use PHPUnit\Framework\Constraint\IsTrue;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use function array_merge;

final class BatchConsumerTest extends TestCase
{
    /**
     * @dataProvider providerMaxBytes
     */
    public function testConsumeWithMaxDuration(?int $maxBytes) : void
    {
        $maxDuration = 30;
        $now = new DateTimeImmutable();
        $clock = new FrozenClock($now);

        $consumerVO = new Consumer();

        $firstMessages = [new Message(), new Message()];
        $secondMessages = [new Message()];
        $thirdMessages = [];
        $fourthMessages = [];
        $fifthMessages = [new Message()];

        $client = Mockery::mock(RestClient::class);
        $client->shouldReceive('getConsumerMessages')
            ->once()
            ->with($consumerVO, $maxDuration, $maxBytes)
            ->andReturnUsing(static function () use (&$now, $clock, $firstMessages) {
                $now = $now->add(new DateInterval('PT20S'));
                $clock->setTo($now);

                return $firstMessages;
            });
        $client->shouldReceive('getConsumerMessages')
            ->once()
            ->with($consumerVO, $maxDuration - 20, $maxBytes)
            ->andReturnUsing(static function () use (&$now, $clock, $secondMessages) {
                $now = $now->add(new DateInterval('PT10S'));
                $clock->setTo($now);

                return $secondMessages;
            });
        $client->shouldReceive('getConsumerMessages')
            ->once()
            ->with($consumerVO, $maxDuration, $maxBytes)
            ->andReturnUsing(static function () use (&$now, $clock, $thirdMessages) {
                $now = $now->add(new DateInterval('PT15S'));
                $clock->setTo($now);

                return $thirdMessages;
            });
        $client->shouldReceive('getConsumerMessages')
            ->once()
            ->with($consumerVO, $maxDuration - 15, $maxBytes)
            ->andReturnUsing(static function () use (&$now, $clock, $fourthMessages) {
                $now = $now->add(new DateInterval('PT20S'));
                $clock->setTo($now);

                return $fourthMessages;
            });
        $client->shouldReceive('getConsumerMessages')
            ->once()
            ->with($consumerVO, $maxDuration, $maxBytes)
            ->andReturnUsing(static function () use (&$now, $clock, $fifthMessages) {
                $now = $now->add(new DateInterval('PT10S'));
                $clock->setTo($now);

                return $fifthMessages;
            });
        $client->shouldReceive('getConsumerMessages')
            ->once()
            ->with($consumerVO, $maxDuration - 25, $maxBytes)
            ->andThrow(new Exception('some exception'));

        $client->shouldReceive('deleteConsumer')
            ->once()
            ->with($consumerVO);

        $consumer = new BatchConsumer($client, $consumerVO, null, $maxDuration, $clock);
        $messages = $consumer->consume($maxBytes);

        self::assertInstanceOf(Generator::class, $messages);
        self::assertTrue($messages->valid());

        $batch = $messages->current();
        self::assertInstanceOf(MessagesBatch::class, $batch);

        self::assertSame(3, $batch->count());
        self::assertCount(3, $batch->getMessages());
        self::assertSame(array_merge($firstMessages, $secondMessages), $batch->getMessages());

        $messages->next();
        self::assertTrue($messages->valid());
        $batch = $messages->current();
        self::assertInstanceOf(MessagesBatch::class, $batch);
        self::assertSame(0, $batch->count());
        self::assertCount(0, $batch->getMessages());

        try {
            $messages->next();
            self::fail('Expected an exception');
        } catch (ConsumerClosed $exception) {
            self::assertFalse($messages->valid());
        }
    }

    /**
     * @dataProvider providerMaxBytes
     */
    public function testConsumeWithMaxCount(?int $maxBytes) : void
    {
        $clock = new FrozenClock(new DateTimeImmutable());

        $consumerVO = new Consumer();

        $firstMessages = [new Message(), new Message()];
        $secondMessages = [new Message()];
        $thirdMessages = [];
        $fourthMessages = [new Message()];

        $client = Mockery::mock(RestClient::class);
        $client->shouldReceive('getConsumerMessages')
            ->once()
            ->with($consumerVO, 10, $maxBytes)
            ->andReturn($firstMessages);
        $client->shouldReceive('getConsumerMessages')
            ->once()
            ->with($consumerVO, 10, $maxBytes)
            ->andReturn($secondMessages);
        $client->shouldReceive('getConsumerMessages')
            ->once()
            ->with($consumerVO, 10, $maxBytes)
            ->andReturn($thirdMessages);
        $client->shouldReceive('getConsumerMessages')
            ->once()
            ->with($consumerVO, 10, $maxBytes)
            ->andReturn($fourthMessages);
        $client->shouldReceive('getConsumerMessages')
            ->once()
            ->with($consumerVO, 10, $maxBytes)
            ->andThrow(new Exception('some exception'));

        $client->shouldReceive('deleteConsumer')
            ->once()
            ->with($consumerVO);

        $consumer = new BatchConsumer($client, $consumerVO, 3, null, $clock);
        $messages = $consumer->consume($maxBytes);

        self::assertInstanceOf(Generator::class, $messages);
        self::assertTrue($messages->valid());

        $batch = $messages->current();
        self::assertInstanceOf(MessagesBatch::class, $batch);

        self::assertSame(3, $batch->count());
        self::assertCount(3, $batch->getMessages());
        self::assertSame(array_merge($firstMessages, $secondMessages), $batch->getMessages());

        try {
            $messages->next();
            self::fail('Expected an exception');
        } catch (ConsumerClosed $exception) {
            self::assertFalse($messages->valid());
        }
    }

    /**
     * @dataProvider providerMaxBytes
     */
    public function testConsumeWithMaxCountAndMaxDuration(?int $maxBytes) : void
    {
        $maxDuration = 30;
        $now = new DateTimeImmutable();
        $clock = new FrozenClock($now);

        $consumerVO = new Consumer();

        $firstMessages = [new Message(), new Message()];
        $secondMessages = [new Message()];
        $thirdMessages = [new Message()];
        $fourthMessages = [new Message()];

        $client = Mockery::mock(RestClient::class);
        $client->shouldReceive('getConsumerMessages')
            ->once()
            ->with($consumerVO, $maxDuration, $maxBytes)
            ->andReturnUsing(static function () use (&$now, $clock, $firstMessages) {
                $now = $now->add(new DateInterval('PT20S'));
                $clock->setTo($now);

                return $firstMessages;
            });
        $client->shouldReceive('getConsumerMessages')
            ->once()
            ->with($consumerVO, $maxDuration - 20, $maxBytes)
            ->andReturnUsing(static function () use (&$now, $clock, $secondMessages) {
                $now = $now->add(new DateInterval('PT5S'));
                $clock->setTo($now);

                return $secondMessages;
            });
        $client->shouldReceive('getConsumerMessages')
            ->once()
            ->with($consumerVO, $maxDuration, $maxBytes)
            ->andReturnUsing(static function () use (&$now, $clock, $thirdMessages) {
                $now = $now->add(new DateInterval('PT40S'));
                $clock->setTo($now);

                return $thirdMessages;
            });
        $client->shouldReceive('getConsumerMessages')
            ->once()
            ->with($consumerVO, $maxDuration, $maxBytes)
            ->andReturnUsing(static function () use (&$now, $clock, $fourthMessages) {
                $now = $now->add(new DateInterval('PT10S'));
                $clock->setTo($now);

                return $fourthMessages;
            });
        $client->shouldReceive('getConsumerMessages')
            ->once()
            ->with($consumerVO, $maxDuration - 10, $maxBytes)
            ->andThrow(new Exception('some exception'));

        $client->shouldReceive('deleteConsumer')
            ->once()
            ->with($consumerVO);

        $consumer = new BatchConsumer($client, $consumerVO, 3, $maxDuration, $clock);
        $messages = $consumer->consume($maxBytes);

        self::assertInstanceOf(Generator::class, $messages);
        self::assertTrue($messages->valid());

        $batch = $messages->current();
        self::assertInstanceOf(MessagesBatch::class, $batch);

        self::assertSame(3, $batch->count());
        self::assertCount(3, $batch->getMessages());
        self::assertSame(array_merge($firstMessages, $secondMessages), $batch->getMessages());

        $messages->next();
        self::assertTrue($messages->valid());
        $batch = $messages->current();
        self::assertInstanceOf(MessagesBatch::class, $batch);

        self::assertSame(1, $batch->count());
        self::assertCount(1, $batch->getMessages());
        self::assertSame($thirdMessages, $batch->getMessages());

        try {
            $messages->next();
            self::fail('Expected an exception');
        } catch (ConsumerClosed $exception) {
            self::assertFalse($messages->valid());
        }
    }

    /**
     * @return iterable<array<mixed>>
     */
    public function providerMaxBytes() : iterable
    {
        yield [null];
        yield [10000];
    }

    public function testCantCreateBatchConsumerWithoutBatchLimitation() : void
    {
        $client = Mockery::mock(RestClient::class);
        $consumerVO = new Consumer();

        $this->expectException(AssertionFailedException::class);
        $this->expectExceptionMessage('You must specify at least one of maxCount/maxDuration');

        new BatchConsumer($client, $consumerVO, null, null);
    }

    public function testCommit() : void
    {
        $message1 = new Message();
        $message1->topic = 'some-topic';
        $message1->partition = 0;
        $message1->offset = 12;

        $message2 = new Message();
        $message2->topic = 'some-topic';
        $message2->partition = 1;
        $message2->offset = 5;

        $message3 = new Message();
        $message3->topic = 'some-other-topic';
        $message3->partition = 0;
        $message3->offset = 31;

        $message4 = new Message();
        $message4->topic = 'some-topic';
        $message4->partition = 0;
        $message4->offset = 13;

        $message5 = new Message();
        $message5->topic = 'some-other-topic';
        $message5->partition = 0;
        $message5->offset = 32;

        $consumerVO = new Consumer();
        $client = Mockery::mock(RestClient::class);
        $client->shouldReceive('consumerCommitOffsets')
            ->with($consumerVO, IsEqual::equalTo([
                new Offset('some-other-topic', 0, 32),
                new Offset('some-topic', 0, 13),
                new Offset('some-topic', 1, 5),
            ]));

        $client->shouldReceive('deleteConsumer')
            ->once()
            ->with($consumerVO);

        $consumer = new BatchConsumer($client, $consumerVO, 100);

        $batch = new MessagesBatch();
        $batch->add($message1);
        $batch->add($message2);
        $batch->add($message3);
        $batch->add($message4);
        $batch->add($message5);

        $consumer->commit($batch);
        self::assertThat(true, new IsTrue());
    }

    public function testCloseAfterError() : void
    {
        $consumerVO = new Consumer();
        $client = Mockery::mock(RestClient::class);
        $client->shouldReceive('getConsumerMessages')
            ->once()
            ->with($consumerVO, 10, null)
            ->andThrow($expectedException = new RuntimeException('some error'));

        $client->shouldReceive('deleteConsumer')
            ->once()
            ->with($consumerVO);

        $consumer = new BatchConsumer($client, $consumerVO, 100);

        $messages = $consumer->consume();
        self::assertInstanceOf(Generator::class, $messages);

        try {
            $messages->current();
            self::fail('Expected an exception');
        } catch (ConsumerClosed $exception) {
            self::assertSame('Failed to consume messages; consumer is now closed', $exception->getMessage());
            self::assertSame($expectedException, $exception->getPrevious());
        }

        $messages = $consumer->consume();
        self::assertInstanceOf(Generator::class, $messages);

        try {
            $messages->current();
            self::fail('Expected an exception');
        } catch (ConsumerClosed $exception) {
            self::assertSame('Consumer is closed', $exception->getMessage());
            self::assertNull($exception->getPrevious());
        }
    }
}
