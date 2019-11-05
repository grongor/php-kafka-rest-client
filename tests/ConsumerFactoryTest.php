<?php

declare(strict_types=1);

namespace Grongor\KafkaRest\Tests;

use Grongor\KafkaRest\Api\RestClient;
use Grongor\KafkaRest\Api\Value\Request\ConsumerOptions;
use Grongor\KafkaRest\Api\Value\Request\Subscription;
use Grongor\KafkaRest\Api\Value\Response\Consumer;
use Grongor\KafkaRest\ConsumerFactory;
use Mockery;
use PHPUnit\Framework\Constraint\IsTrue;
use PHPUnit\Framework\TestCase;

final class ConsumerFactoryTest extends TestCase
{
    /**
     * @dataProvider providerCreate
     */
    public function testCreate(?ConsumerOptions $consumerOptions) : void
    {
        $group = 'test-group';
        $subscription = Subscription::topic('some-topic');

        $consumer = new Consumer();

        $client = Mockery::mock(RestClient::class);
        $client->shouldReceive('createConsumer')
            ->once()
            ->with($group, $consumerOptions)
            ->andReturn($consumer);

        $client->shouldReceive('consumerSubscribe')
            ->once()
            ->with($consumer, $subscription);

        $client->shouldReceive('deleteConsumer')
            ->with($consumer);

        $factory = new ConsumerFactory($client);
        $factory->create($group, $subscription, $consumerOptions);
        self::assertThat(true, new IsTrue());
    }

    /**
     * @return iterable<array<mixed>>
     */
    public function providerCreate() : iterable
    {
        yield [null];
        yield [new ConsumerOptions()];
    }
}
