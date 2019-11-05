<?php

declare(strict_types=1);

namespace Grongor\KafkaRest;

use Grongor\KafkaRest\Api\RestClient;
use Grongor\KafkaRest\Api\Value\Request\ConsumerOptions;
use Grongor\KafkaRest\Api\Value\Request\Subscription;

final class ConsumerFactory
{
    /** @var RestClient */
    private $client;

    public function __construct(RestClient $client)
    {
        $this->client = $client;
    }

    public function create(
        string $group,
        Subscription $subscription,
        ?ConsumerOptions $consumerOptions = null
    ) : Consumer {
        $consumer = $this->client->createConsumer($group, $consumerOptions);
        $this->client->consumerSubscribe($consumer, $subscription);

        return new Consumer($this->client, $consumer);
    }
}
