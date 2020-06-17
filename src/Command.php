<?php
declare(strict_types=1);

namespace Rabbit\DB\Redisql;

use Exception;
use rabbit\db\Command as BaseCommand;
use rabbit\db\Exception as DbException;

/**
 * Class Command
 * @package rabbit\db\click
 * @property $db \rabbit\db\click\Connection
 */
class Command extends BaseCommand
{
    const FETCH = 'fetch';
    const FETCH_ALL = 'fetchAll';
    const FETCH_COLUMN = 'fetchColumn';
    const FETCH_SCALAR = 'fetchScalar';

    /** @var int fetch type result */
    public $fetchMode = 0;

    /**
     * @param array $values
     * @return $this|BaseCommand
     */
    public function bindValues($values)
    {
        if (empty($values)) {
            return $this;
        }
        foreach ($values as $name => $value) {
            $this->params[$name] = $value;
        }

        return $this;
    }

    /**
     * @return int
     * @throws Exception
     */
    public function execute()
    {
        $rawSql = $this->getRawSql();
        $this->logQuery($rawSql, 'redisql');
        $dbname = $this->db->dbName;
        $conn = $this->db->getConn();
        try {
            if (empty($this->params)) {
                $data = $conn->executeCommand("REDISQL.EXEC", [$dbname, $rawSql]);
            } else {
                $stmt = md5($this->_sql);
                if ($conn->setnx($stmt, 1)) {
                    $conn->executeCommand("REDISQL.CREATE_STATEMENT", [$dbname, $stmt, $this->_sql]);
                }
                $data = $conn->executeCommand('REDISQL.EXEC_STATEMENT', [$dbname, $stmt, array_values($this->params)]);
            }
            if ($data[0] === 'DONE' && $data[1] === '0') {
                return false;
            }
            return $data;
        } catch (Exception $e) {
            throw new DbException("ActiveQuery error: " . $e->getMessage());
        } finally {
            $conn->release(true);
        }
    }


    /**
     * @return array|mixed|null
     * @throws DbException
     */
    public function queryColumn()
    {
        return $this->queryInternal(self::FETCH_COLUMN);
    }

    /**
     * @return array|false|int|mixed|string|null
     * @throws DbException
     */
    public function queryScalar()
    {
        $result = $this->queryInternal(self::FETCH_SCALAR, 0);
        if (is_array($result)) {
            return current($result);
        } else {
            return $result;
        }
    }

    /**
     * @return string
     */
    public function getRawSql()
    {
        if (empty($this->params)) {
            return $this->_sql;
        }
        $params = [];
        foreach ($this->params as $name => $value) {
            if (is_string($value)) {
                $params[$name] = $value;
            } elseif (is_bool($value)) {
                $params[$name] = ($value ? 'TRUE' : 'FALSE');
            } elseif ($value === null) {
                $params[$name] = 'NULL';
            } elseif (!is_object($value) && !is_resource($value)) {
                $params[$name] = $value;
            }
        }
        if (!isset($params[0])) {
            return strtr($this->_sql, $params);
        }
        $sql = '';
        foreach (explode('?', $this->_sql) as $i => $part) {
            $sql .= $part . (isset($params[$i]) ? $params[$i] : '');
        }
        return $sql;
    }

    /**
     * @param string $method
     * @param null $fetchMode
     * @return array|mixed|null
     * @throws DbException
     * @throws Exception
     */
    protected function queryInternal($method, $fetchMode = null)
    {
        $rawSql = $this->getRawSql();
        $this->logQuery($rawSql);

        $dbname = $this->db->dbName;
        $conn = $this->db->getConn();
        try {
            if (empty($this->params)) {
                $data = $conn->executeCommand("REDISQL.QUERY", [$dbname, $rawSql]);
            } else {
                $stmt = md5($this->_sql);
                if ($conn->setnx($stmt, 1)) {
                    $conn->executeCommand("REDISQL.CREATE_STATEMENT", [$dbname, $stmt, $this->_sql]);
                }
                $data = $conn->executeCommand('REDISQL.QUERY_STATEMENT', [$dbname, $stmt, array_values($this->params)]);
            }
            if ($data[0] === 'DONE' && $data[1] === '0') {
                return false;
            }
            return $this->prepareResult($data, $method);
        } catch (Exception $e) {
            throw new DbException("ActiveQuery error: " . $e->getMessage());
        } finally {
            $conn->release(true);
        }
    }

    /**
     * @param $result
     * @param null $method
     * @return array|mixed|null
     */
    private function prepareResult($result, $method = null)
    {
        switch ($method) {
            case self::FETCH_COLUMN:
                return array_map(function ($a) {
                    return array_values($a)[0];
                }, $result);
                break;
            case self::FETCH_SCALAR:
                if (array_key_exists(0, $result)) {
                    return current($result[0]);
                }
                break;
            case self::FETCH:
                return is_array($result) ? array_shift($result) : $result;
                break;
        }

        return $result;
    }
}
