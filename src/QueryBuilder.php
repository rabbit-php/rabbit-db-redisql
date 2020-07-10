<?php
declare(strict_types=1);

namespace Rabbit\DB\Redisql;


use Rabbit\Base\Exception\NotSupportedException;
use Rabbit\DB\Exception;
use Rabbit\DB\ExpressionInterface;
use Rabbit\DB\Query;
use Throwable;

/**
 * Class QueryBuilder
 * @package Rabbit\DB\Redisql
 */
class QueryBuilder extends \Rabbit\DB\QueryBuilder
{
    /**
     * @param string|null $value
     * @param array $params
     * @return string
     */
    public function bindParam(?string $value, array &$params): string
    {
        $phName = '?' . (count($params) + 1);
        $params[$phName] = $value;
        return $phName;
    }

    /**
     * {@inheritdoc}
     */
    protected function defaultExpressionBuilders(): array
    {
        return array_merge(parent::defaultExpressionBuilders(), [
            \Rabbit\DB\PdoValueBuilder::class => ValueBuilder::class,
        ]);
    }

    /**
     * @param Query $query
     * @param array $params
     * @return array
     * @throws Exception
     * @throws Throwable
     */
    public function build(Query $query, array $params = []): array
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
     * @param string|null $selectOption
     * @return string
     * @throws Exception
     * @throws Throwable
     */
    public function buildSelect(array $columns, array &$params, bool $distinct = false, string $selectOption = null)
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
