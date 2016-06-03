<?php

namespace VladimirYuldashev\LaravelQueueRabbitMQ\Queue\Connectors;

use Illuminate\Queue\Connectors\ConnectorInterface;
use PhpAmqpLib\Connection\AMQPConnection;
use VladimirYuldashev\LaravelQueueRabbitMQ\Queue\RabbitMQQueue;

class RabbitMQConnector implements ConnectorInterface
{

	/**
	 * Establish a queue connection.
	 *
	 * @param  array $config
	 *
	 * @return \Illuminate\Queue\QueueInterface
	 */
	public function connect(array $config)
	{
		// create connection with AMQP
		try {
			$connection = new AMQPConnection($config['host'], $config['port'], $config['login'], $config['password'], $config['vhost']);
		}
		catch (\PhpAmqpLib\Exception\AMQPRuntimeException $e)
		{
			$max = 120;
			$file = storage_path('cache').'/ampq';
			if (is_file($file) && (time() - filemtime($file) < $max)) {
				$timeout = file_get_contents($file);
			} else {
				$timeout = 1;
			}
			sleep($timeout);
			file_put_contents($file, min($max, $timeout * 2));
			throw $e;
		}

		return new RabbitMQQueue(
			$connection,
			$config
		);
	}
}
