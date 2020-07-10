<?php
declare(strict_types=1);

namespace Rabbit\DB\Redisql;

use DI\DependencyException;
use DI\NotFoundException;
use Rabbit\Base\App;
use Rabbit\DB\Redis\Redis;
use Rabbit\Pool\PoolManager;
use Throwable;

/**
 * Class Connection
 * @package Rabbit\DB\Redisql
 */
class Connection extends \Rabbit\DB\Connection
{
    public array $schemaMap = [

    ];
    protected string $commandClass = Command::class;
    /** @var string */
    public ?string $dbName = null;

    /**
     * Connection constructor.
     * @param Redis $redis
     */
    public function __construct(Redis $redis)
    {
        parent::__construct($redis->getPool()->getConnectionAddress());
        isset($this->parseDsn['query']) ? parse_str($this->parseDsn['query'], $this->parseDsn['query']) : $this->parseDsn['query'] = [];
        $this->dbName = $this->parseDsn['query']['dbname'];
        $this->poolKey = $redis->getPool()->getPoolConfig()->getName();
        $this->driver = 'redisql';
    }

    /**
     * @return mixed|null
     */
    public function getConn()
    {
        return PoolManager::getPool($this->poolKey)->get();
    }

    /**
     * @return mixed|\rabbit\db\Schema
     * @throws DependencyException
     * @throws NotFoundException
     */
    public function getSchema(): \Rabbit\DB\Schema
    {
        if ($this->_schema !== null) {
            return $this->_schema;
        }
        return $this->_schema = create([
            'class' => Schema::class,
            'db' => $this
        ]);
    }

    /**
     * @param string $value
     * @return string
     */
    public function quoteValue(string $value): string
    {
        return $value;
    }

    /**
     * @param string $sql
     * @return string
     */
    public function quoteSql(string $sql): string
    {
        return $sql;
    }

    /**
     * @return array
     * @throws Throwable
     */
    public function __sleep()
    {
        $this->close();
        return array_keys(get_object_vars($this));
    }

    /**
     * @throws Throwable
     */
    public function close(): void
    {
        if ($this->getIsActive()) {
            App::warning('Closing DB connection: ' . $this->shortDsn, 'redisql');
        }
    }

    /**
     * @return bool
     */
    public function getIsActive(): bool
    {
        return false;
    }

    /**
     * @param string $name
     * @return string
     */
    public function quoteTableName(string $name): string
    {
        return $name;
    }

    /**
     * @return string
     */
    public function getDriverName(): string
    {
        return 'redisql';
    }

    /**
     * @param string $name
     * @return string
     */
    public function quoteColumnName(string $name): string
    {
        return $name;
    }
}