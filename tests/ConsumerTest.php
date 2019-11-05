<?php

declare(strict_types=1);

namespace Grongor\KafkaRest\Tests;

use Exception;
use Generator;
use Grongor\KafkaRest\Api\RestClient;
use Grongor\KafkaRest\Api\Value\Request\Offset;
use Grongor\KafkaRest\Api\Value\Response\Message;
use Grongor\KafkaRest\Consumer;
use Grongor\KafkaRest\Exception\ConsumerClosed;
use Hamcrest\Core\IsEqual;
use Mockery;
use PHPUnit\Framework\Constraint\IsTrue;
use PHPUnit\Framework\TestCase;
use RuntimeException;

final class ConsumerTest extends TestCase
{
    /**
     * @dataProvider providerConsume
     */
    public function testConsume(?int $timeout, ?int $maxBytes) : void
    {
        $consumerVO = new \Grongor\KafkaRest\Api\Value\Response\Consumer();

        $firstMessages = [new Message(), new Message()];
        $secondMessages = [new Message()];

        $client = Mockery::mock(RestClient::class);
        $client->shouldReceive('getConsumerMessages')
            ->once()
            ->with($consumerVO, $timeout, $maxBytes)
            ->andReturns($firstMessages);
        $client->shouldReceive('getConsumerMessages')
            ->once()
            ->with($consumerVO, $timeout, $maxBytes)
            ->andReturns($secondMessages);
        $client->shouldReceive('getConsumerMessages')
            ->once()
            ->andThrow(new Exception('some exception'));

        $client->shouldReceive('deleteConsumer')
            ->once()
            ->with($consumerVO);

        $consumer = new Consumer($client, $consumerVO);
        $messages = $consumer->consume($timeout, $maxBytes);

        self::assertInstanceOf(Generator::class, $messages);

        self::assertTrue($messages->valid());
        self::assertSame($firstMessages[0], $messages->current());

        $messages->next();
        self::assertTrue($messages->valid());
        self::assertSame($firstMessages[1], $messages->current());

        $messages->next();
        self::assertTrue($messages->valid());
        self::assertSame($secondMessages[0], $messages->current());
        self::assertTrue($messages->valid());

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
    public function providerConsume() : iterable
    {
        yield [null, null];
        yield [null, 10000];
        yield [10, null];
    }

    public function testCommit() : void
    {
        $message = new Message();
        $message->topic = 'some-topic';
        $message->partition = 0;
        $message->offset = 12;

        $consumerVO = new \Grongor\KafkaRest\Api\Value\Response\Consumer();
        $client = Mockery::mock(RestClient::class);
        $client->shouldReceive('consumerCommitOffsets')
            ->once()
            ->with($consumerVO, IsEqual::equalTo([new Offset('some-topic', 0, 12)]));

        $client->shouldReceive('deleteConsumer')
            ->once()
            ->with($consumerVO);

        $consumer = new Consumer($client, $consumerVO);
        $consumer->commit($message);
        self::assertThat(true, new IsTrue());
    }

    public function testCloseAfterError() : void
    {
        $consumerVO = new \Grongor\KafkaRest\Api\Value\Response\Consumer();
        $client = Mockery::mock(RestClient::class);
        $client->shouldReceive('getConsumerMessages')
            ->once()
            ->with($consumerVO, null, null)
            ->andThrow($expectedException = new RuntimeException('some error'));

        $client->shouldReceive('deleteConsumer')
            ->once()
            ->with($consumerVO);

        $consumer = new Consumer($client, $consumerVO);

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
