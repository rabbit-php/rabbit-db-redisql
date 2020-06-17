<?php
declare(strict_types=1);

namespace Rabbit\DB\Redisql;

use rabbit\db\ExpressionInterface;
use rabbit\db\Query;
use rabbit\exception\InvalidConfigException;
use rabbit\exception\NotSupportedException;

/**
 * Class QueryBuilder
 * @package Rabbit\DB\Redisql
 */
class QueryBuilder extends \rabbit\db\QueryBuilder
{
    /**
     * @param string|null $value
     * @param array $params
     * @return string
     */
    public function bindParam($value, &$params)
    {
        $phName = '?' . (count($params) + 1);
        $params[$phName] = $value;
        return $phName;
    }

    /**
     * {@inheritdoc}
     */
    protected function defaultExpressionBuilders()
    {
        return array_merge(parent::defaultExpressionBuilders(), [
            \rabbit\db\PdoValueBuilder::class => ValueBuilder::class,
        ]);
    }

    /**
     * @param Query $query
     * @param array $params
     * @return array
     * @throws NotSupportedException
     * @throws InvalidConfigException
     */
    public function build($query, $params = [])
    {
        if ($query instanceof ActiveQuery && empty($query->select)) {
            $query->select($query->model->attributes());
        }
        return parent::build($query, $params);
    }

    /**
     * @param array $columns
     * @param array $params
     * @param bool $distinct
     * @param null $selectOption
     * @return string
     * @throws NotSupportedException|InvalidConfigException
     */
    public function buildSelect($columns, &$params, $distinct = false, $selectOption = null)
    {
        $select = $distinct ? 'SELECT DISTINCT' : 'SELECT';
        if ($selectOption !== null) {
            $select .= ' ' . $selectOption;
        }

        if (empty($columns)) {
            throw new NotSupportedException("Redisql not support !array select");
        }

        foreach ($columns as $i => $column) {
            if ($column instanceof ExpressionInterface) {
                if (is_int($i)) {
                    $columns[$i] = $this->buildExpression($column, $params);
                } else {
                    $columns[$i] = $this->buildExpression($column, $params) . ' AS ' . $this->db->quoteColumnName($i);
                }
            } elseif ($column instanceof Query) {
                [$sql, $params] = $this->build($column, $params);
                $columns[$i] = "($sql) AS " . $this->db->quoteColumnName($i);
            } elseif (is_string($i)) {
                if (strpos($column, '(') === false) {
                    $column = $this->db->quoteColumnName($column);
                }
                $columns[$i] = "$column AS " . $this->db->quoteColumnName($i);
            } elseif (strpos($column, '(') === false) {
                if (preg_match('/^(.*?)(?i:\s+as\s+|\s+)([\w\-_\.]+)$/', $column, $matches)) {
                    $columns[$i] = $this->db->quoteColumnName($matches[1]) . ' AS ' . $this->db->quoteColumnName($matches[2]);
                } else {
                    $columns[$i] = $this->db->quoteColumnName($column);
                }
            }
        }

        return $select . ' ' . implode(', ', $columns);
    }
}
