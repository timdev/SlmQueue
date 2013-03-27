<?php

namespace SlmQueue\Worker;

use SlmQueue\Job\JobInterface;
use SlmQueue\Options\WorkerOptions;
use SlmQueue\Queue\QueueInterface;
use SlmQueue\Queue\QueuePluginManager;
use Zend\EventManager\EventManagerInterface;
use Zend\EventManager\EventManagerAwareInterface;
use Zend\EventManager\EventManager;

/**
 * AbstractWorker
 */
abstract class AbstractWorker implements WorkerInterface, EventManagerAwareInterface
{
    /**
     * @var QueuePluginManager
     */
    protected $queuePluginManager;

    /**
     * @var bool
     */
    protected $stopped = false;

    /**
     * @var WorkerOptions
     */
    protected $options;

    /**
     * @var EventManagerInterface
     */
    protected $events;

    /**
     * Constructor
     *
     * @param QueuePluginManager $queuePluginManager
     * @param WorkerOptions      $options
     */
    public function __construct(QueuePluginManager $queuePluginManager, WorkerOptions $options)
    {
        $this->queuePluginManager = $queuePluginManager;
        $this->options            = $options;

        // Listen to the signals SIGTERM and SIGINT so that the worker can be killed properly. Note that
        // because pcntl_signal may not be available on Windows, we needed to check for the existence of the function
        if (function_exists('pcntl_signal')) {
            declare(ticks = 1);
            pcntl_signal(SIGTERM, array($this, 'handleSignal'));
            pcntl_signal(SIGINT,  array($this, 'handleSignal'));
        }
    }

  /**
   * Set the event manager instance used by this context
   *
   * @param  EventManagerInterface $events
   * @return mixed
   */
  public function setEventManager(EventManagerInterface $events)
  {
    $identifiers = array(__CLASS__, get_called_class());
    if (isset($this->eventIdentifier)) {
      if ((is_string($this->eventIdentifier))
        || (is_array($this->eventIdentifier))
        || ($this->eventIdentifier instanceof Traversable)
      ) {
        $identifiers = array_unique(array_merge($identifiers, (array) $this->eventIdentifier));
      } elseif (is_object($this->eventIdentifier)) {
        $identifiers[] = $this->eventIdentifier;
      }
      // silently ignore invalid eventIdentifier types
    }
    $events->setIdentifiers($identifiers);
    $this->events = $events;
    return $this;
  }

  /**
   * Retrieve the event manager
   *
   * Lazy-loads an EventManager instance if none registered.
   *
   * @return EventManagerInterface
   */
  public function getEventManager()
  {
    if (!$this->events instanceof EventManagerInterface) {
      $this->setEventManager(new EventManager());
    }
    return $this->events;
  }

    /**
     * {@inheritDoc}
     */
    public function processQueue($queueName, array $options = array())
    {
        /** @var $queue QueueInterface */
        $queue = $this->queuePluginManager->get($queueName);
        $count = 0;

        while (true) {
            // Pop operations may return a list of jobs or a single job
            $jobs = $queue->pop($options);

            if (!is_array($jobs)) {
                $jobs = array($jobs);
            }

            foreach ($jobs as $job) {
                // The queue may return null, for instance if a timeout was set
                if (!$job instanceof JobInterface) {
                    return $count;
                }

                $this->processJob($job, $queue);
                $count++;

                // Those are various criterias to stop the queue processing
                if (
                    $count === $this->options->getMaxRuns()
                    || memory_get_usage() > $this->options->getMaxMemory()
                    || $this->isStopped()
                ) {
                    return $count;
                }
            }
        }

        return $count;
    }

    /**
     * Check if the script has been stopped from a signal
     *
     * @return bool
     */
    public function isStopped()
    {
        return $this->stopped;
    }

    /**
     * Handle the signal
     *
     * @param int $signo
     */
    public function handleSignal($signo)
    {
        switch($signo) {
            case SIGTERM:
            case SIGINT:
                $this->getEventManager()->trigger(__FUNCTION__, $this, compact('signo'));
                $this->stopped = true;
                break;
        }
    }
}
