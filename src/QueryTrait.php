<?php
declare(strict_types=1);

namespace Rabbit\DB\Redisql;

use Rabbit\Base\Exception\NotSupportedException;
use Rabbit\DB\ConnectionInterface;

/**
 * Class QueryTrait
 * @package Rabbit\DB\Redisql
 */
trait QueryTrait
{
    /** @var string */
    protected string $queryBuilder = QueryBuilder::class;

    /**
     * @param $columns
     * @param string $option
     * @return $this
     * @throws NotSupportedException
     */
    public function select($columns, string $option = null): self
    {
        if (!is_array($columns)) {
            throw new NotSupportedException("Redisql not support !array select");
        }
        foreach ($columns as $column) {
            if (strpos($column, '*') !== false) {
                throw new NotSupportedException("Redisql not support select *");
            }
        }
        $this->select = [];
        $this->select = $this->getUniqueColumns($columns);
        $this->selectOption = $option;
        return $this;
    }

    /**
     * @param ConnectionInterface $db
     * @return bool|mixed|null
     */
    public function one(ConnectionInterface $db = null)
    {
        if ($this->emulateExecution) {
            return false;
        }
        $this->limit(1);
        $result = $this->createCommand($db)->queryOne();
        if ($result) {
            $list[] = $result;
            $result = current($this->buildWith($list));
        }
        if ($result !== false) {
            $res = [];
            foreach ($this->select as $index => $name) {
                $name = explode(' ', $name);
                $res[end($name)] = $result[$index];
            }
            $models = $this->populate([$res]);
            return reset($models) ?: null;
        }
        return null;
    }

    /**
     * @param ConnectionInterface $db
     * @return array
     */
    public function all(ConnectionInterface $db = null): array
    {
        if ($this->emulateExecution) {
            return [];
        }
        $rows = $this->createCommand($db)->queryAll();
        foreach ($rows as $i => $row) {
            foreach ($this->select as $index => $name) {
                $name = explode(' ', $name);
                $res[end($name)] = $row[$index];
            }
            $rows[$i] = $res;
        }
        $rows = $this->populate($rows);
        return $rows;
    }
}