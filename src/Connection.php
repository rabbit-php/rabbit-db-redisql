<?php
declare(strict_types=1);

namespace Rabbit\DB\Redisql;

use DI\DependencyException;
use DI\NotFoundException;
use rabbit\activerecord\ActiveRecord;
use rabbit\App;
use rabbit\core\ObjectFactory;
use rabbit\db\Exception;
use rabbit\db\redis\Redis;
use rabbit\exception\InvalidConfigException;
use rabbit\helper\ArrayHelper;
use rabbit\helper\JsonHelper;
use rabbit\pool\ConnectionInterface;
use rabbit\pool\PoolManager;

/**
 * Class Connection
 * @package Rabbit\DB\Redisql
 */
class Connection extends \rabbit\db\Connection implements ConnectionInterface
{
    public $schemaMap = [

    ];
    protected $commandClass = Command::class;
    /** @var string */
    public $dbName;
    /** @var \rabbit\db\QueryBuilder */
    private $builder;

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
        return PoolManager::getPool($this->poolKey)->getConnection();
    }

    /**
     * @return \rabbit\db\QueryBuilder
     */
    public function getQueryBuilder(): \rabbit\db\QueryBuilder
    {
        if ($this->builder === null) {
            $this->builder = new QueryBuilder($this);
        }

        return $this->builder;
    }

    /**
     * @return mixed|\rabbit\db\Schema
     * @throws DependencyException
     * @throws NotFoundException
     */
    public function getSchema()
    {
        if ($this->_schema !== null) {
            return $this->_schema;
        }
        return $this->_schema = ObjectFactory::createObject([
            'class' => Schema::class,
            'db' => $this
        ]);
    }

    /**
     * @param string $str
     * @return string
     */
    public function quoteValue($str)
    {
        return $str;
    }

    /**
     * @param string $sql
     * @return string
     */
    public function quoteSql($sql)
    {
        return $sql;
    }

    /**
     * @return array
     * @throws \Exception
     */
    public function __sleep()
    {
        $this->close();
        return array_keys(get_object_vars($this));
    }

    /**
     * @throws \Exception
     */
    public function close()
    {
        if ($this->getIsActive()) {
            App::warning('Closing DB connection: ' . $this->shortDsn, 'redisql');
        }
    }

    /**
     * @return bool
     */
    public function getIsActive()
    {
        return false;
    }

    /**
     * @param string $name
     * @return string
     */
    public function quoteTableName($name)
    {
        return $name;
    }

    /**
     * @return string
     */
    public function getDriverName()
    {
        return 'redisql';
    }

    /**
     * @param string $name
     * @return string
     */
    public function quoteColumnName($name)
    {
        return $name;
    }

    /**
     * @param ActiveRecord $model
     * @param array $array_columns
     * @return int
     * @throws Exception
     * @throws InvalidConfigException
     */
    public function saveSeveral(ActiveRecord $model, array $array_columns): int
    {
        if (empty($array_columns)) {
            return 0;
        }
        $sql = '';
        $params = [];
        $i = 0;
        if (ArrayHelper::isAssociative($array_columns)) {
            $array_columns = [$array_columns];
        }
        $keys = $model::primaryKey();

        foreach ($array_columns as $item) {
            $table = clone $model;
            $table->load($item, '');
            //关联模型
            foreach ($table->getRelations() as $child => $val) {
                $key = explode("\\", $child);
                $key = strtolower(end($key));
                if (isset($item[$key])) {
                    $child_model = new $child();
                    if (!isset($item[$key][0])) {
                        $item[$key] = [$item[$key]];
                    }
                    foreach ($val as $c_attr => $p_attr) {
                        foreach ($item[$key] as $index => &$param) {
                            $param[$c_attr] = $table->{$p_attr};
                        }
                    }
                    if ($this->saveSeveral($child_model, $item[$key]) === false) {
                        return 0;
                    }
                }
            }
            $names = array();
            $placeholders = array();
            if (!$table->validate()) {
                throw new Exception(implode(BREAKS, $table->getFirstErrors()));
            }
            $tableArray = $table->toArray();
            if ($keys) {
                foreach ($keys as $key) {
                    if (isset($item[$key]) && (!isset($item[$key]) || $tableArray[$key] === null)) {
                        $tableArray[$key] = $item[$key];
                    }
                }
            }
            foreach ($tableArray as $name => $value) {
                if (!$i) {
                    $names[] = $name;
                }
                if (is_array($value)) {
                    $placeholders[] = '?' . $i;
                    $params[] = JsonHelper::encode($value);
                } else {
                    $placeholders[] = '?' . $i;
                    $params[] = $value;
                }
            }
            if (!$i) {
                $sql = 'INSERT INTO ' . $table::tableName()
                    . ' (' . implode(', ', $names) . ') VALUES ('
                    . implode(', ', $placeholders) . ')';
            } else {
                $sql .= ',(' . implode(', ', $placeholders) . ')';
            }
            $i++;
        }
        $result = $table::getDb()->createCommand($sql, $params)->execute();
        if (is_array($result)) {
            return end($result);
        }
        return $result;
    }

    /**
     * @param ActiveRecord $table
     * @param array $array_columns
     * @return int
     * @throws Exception
     * @throws InvalidConfigException
     */
    public function deleteSeveral(ActiveRecord $table, array $array_columns): int
    {
        if (empty($array_columns)) {
            return 0;
        }
        $result = false;
        $keys = $table::primaryKey();
        $condition = [];
        if (ArrayHelper::isAssociative($array_columns)) {
            $array_columns = [$array_columns];
        }
        foreach ($array_columns as $item) {
            $table->load($item, '');
            foreach ($table->getRelations() as $child => $val) {
                $key = explode("\\", $child);
                $key = strtolower(end($key));
                if (isset($item[$key])) {
                    $child_model = new $child();
                    if ($this->deleteSeveral($child_model, $item[$key]) === false) {
                        return 0;
                    }
                }
            }
            if ($keys) {
                foreach ($keys as $key) {
                    if (isset($item[$key])) {
                        $condition[$key][] = $item[$key];
                    }
                }
            }
        }
        if ($condition) {
            $result = $table->deleteAll($condition);
        }
        return $result;
    }
}