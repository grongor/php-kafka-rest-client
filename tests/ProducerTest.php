<?php

declare(strict_types=1);

namespace Grongor\KafkaRest\Tests;

use Grongor\KafkaRest\Api\RestClient;
use Grongor\KafkaRest\Api\Value\Request\Message;
use Grongor\KafkaRest\Api\Value\Response\ProduceResult;
use Grongor\KafkaRest\Exception\FailedToProduceMessages;
use Grongor\KafkaRest\Producer;
use Mockery;
use PHPUnit\Framework\Constraint\IsTrue;
use PHPUnit\Framework\TestCase;

final class ProducerTest extends TestCase
{
    public function testProduce() : void
    {
        $message = new Message('msg1');

        $result = ProduceResult::offset(1, 25);

        $client = Mockery::mock(RestClient::class);
        $client->shouldReceive('produce')
            ->once()
            ->with('some-topic', [$message])
            ->andReturn([$result]);

        $producer = new Producer($client);
        $producer->produce('some-topic', $message);

        self::assertThat(true, new IsTrue());
    }

    public function testProduceWithNonRetryableError() : void
    {
        $message = new Message('msg1');

        $result = ProduceResult::error(1, 'some non-retryable error');

        $client = Mockery::mock(RestClient::class);
        $client->shouldReceive('produce')
            ->once()
            ->with('some-topic', [$message])
            ->andReturn([$result]);

        $producer = new Producer($client);

        try {
            $producer->produce('some-topic', $message);
            self::fail('Expectec an FailedToProduceMessages exception');
        } catch (FailedToProduceMessages $exception) {
            self::assertSame(
                [
                    [
                        'error' => $result->error,
                        'message' => $message,
                    ],
                ],
                $exception->nonRetryable
            );
            self::assertCount(0, $exception->retryable);
        }
    }

    public function testProduceWithRetryableError() : void
    {
        $message = new Message('msg1');

        $result = ProduceResult::error(2, 'some retryable error');

        $client = Mockery::mock(RestClient::class);
        $client->shouldReceive('produce')
            ->once()
            ->with('some-topic', [$message])
            ->andReturn([$result]);

        $producer = new Producer($client);

        try {
            $producer->produce('some-topic', $message);
            self::fail('Expectec an FailedToProduceMessages exception');
        } catch (FailedToProduceMessages $exception) {
            self::assertCount(0, $exception->nonRetryable);
            self::assertSame(
                [
                    [
                        'error' => $result->error,
                        'message' => $message,
                    ],
                ],
                $exception->retryable
            );
        }
    }

    public function testProduceBatch() : void
    {
        $message1 = new Message('msg1');
        $message2 = new Message('msg2');
        $message3 = new Message('msg3');
        $message4 = new Message('msg3');
        $message5 = new Message('msg3');
        $messages = [$message1, $message2, $message3, $message4, $message5];

        $result1 = ProduceResult::offset(1, 25);
        $result2 = ProduceResult::error(1, 'some non-retryable error');
        $result3 = ProduceResult::error(2, 'some retryable error');
        $result4 = ProduceResult::error(1, 'some non-retryable error');
        $result5 = ProduceResult::offset(0, 26);

        $results = [$result1, $result2, $result3, $result4, $result5];

        $client = Mockery::mock(RestClient::class);
        $client->shouldReceive('produce')
            ->once()
            ->with('some-topic', $messages)
            ->andReturn($results);

        $producer = new Producer($client);

        try {
            $producer->produceBatch('some-topic', $messages);
            self::fail('Expectec an FailedToProduceMessages exception');
        } catch (FailedToProduceMessages $exception) {
            self::assertSame(
                [
                    [
                        'error' => $result2->error,
                        'message' => $message2,
                    ],
                    [
                        'error' => $result4->error,
                        'message' => $message4,
                    ],
                ],
                $exception->nonRetryable
            );
            self::assertSame(
                [
                    [
                        'error' => $result3->error,
                        'message' => $message3,
                    ],
                ],
                $exception->retryable
            );
        }

        // no request should be made if there are no messages to produce
        $producer->produceBatch('some-topic', []);
    }
}
