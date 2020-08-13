<?php
declare(strict_types=1);

namespace Rabbit\DB\Redisql;

use Exception;
use Rabbit\DB\DataReader;
use Throwable;

/**
 * Class Command
 * @package Rabbit\DB\Redisql
 */
class Command extends \Rabbit\DB\Command
{
    const FETCH = 'fetch';
    const FETCH_ALL = 'fetchAll';
    const FETCH_COLUMN = 'fetchColumn';
    const FETCH_SCALAR = 'fetchScalar';

    /** @var int fetch type result */
    public int $fetchMode = 0;

    /**
     * @param array $values
     * @return $this
     */
    public function bindValues(array $values): self
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
     * @throws Throwable
     */
    public function execute(): int
    {
        $rawSql = $this->getRawSql();
        $this->logQuery($rawSql, 'redisql');
        $dbname = $this->db->dbName;
        $conn = $this->db->getConn();
        try {
            if (empty($this->params)) {
                $data = $conn->executeCommand("REDISQL.EXEC", [$dbname, $rawSql]);
            } else {
                $stmt = md5($this->sql);
                if ($conn->setnx($stmt, 1)) {
                    $conn->executeCommand("REDISQL.CREATE_STATEMENT", [$dbname, $stmt, $this->sql]);
                }
                $data = $conn->executeCommand('REDISQL.EXEC_STATEMENT', [$dbname, $stmt, array_values($this->params)]);
            }
            if ($data[0] === 'DONE' && $data[1] === '0') {
                return 0;
            }
            return (int)$data;
        } catch (Exception $e) {
            throw new Exception("ActiveQuery error: " . $e->getMessage());
        } finally {
            $conn->release(true);
        }
    }


    /**
     * @return array|null
     * @throws Exception
     */
    public function queryColumn(): ?array
    {
        return $this->queryInternal(self::FETCH_COLUMN);
    }

    /**
     * @return string|null
     * @throws Exception
     */
    public function queryScalar(): ?string
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
    public function getRawSql(): string
    {
        if (empty($this->params)) {
            return $this->sql;
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
            return strtr($this->sql, $params);
        }
        $sql = '';
        foreach (explode('?', $this->sql) as $i => $part) {
            $sql .= $part . (isset($params[$i]) ? $params[$i] : '');
        }
        return $sql;
    }

    /**
     * @param string $method
     * @param int|null $fetchMode
     * @return array|bool|mixed|DataReader|null
     * @throws Throwable
     */
    protected function queryInternal(string $method, int $fetchMode = null)
    {
        $rawSql = $this->getRawSql();
        $this->logQuery($rawSql);

        $dbname = $this->db->dbName;
        $conn = $this->db->getConn();
        try {
            if (empty($this->params)) {
                $data = $conn->executeCommand("REDISQL.QUERY", [$dbname, $rawSql]);
            } else {
                $stmt = md5($this->sql);
                if ($conn->setnx($stmt, 1)) {
                    $conn->executeCommand("REDISQL.CREATE_STATEMENT", [$dbname, $stmt, $this->sql]);
                }
                $data = $conn->executeCommand('REDISQL.QUERY_STATEMENT', [$dbname, $stmt, array_values($this->params)]);
            }
            if ($data[0] === 'DONE' && $data[1] === '0') {
                return false;
            }
            return $this->prepareResult($data, $method);
        } catch (Exception $e) {
            throw new Exception("ActiveQuery error: " . $e->getMessage());
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
