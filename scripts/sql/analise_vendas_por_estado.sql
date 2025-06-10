SELECT
  customer_state AS estado,
  COUNT(DISTINCT order_id) AS total_de_pedidos,
  SUM(total_value) AS valor_total_vendas
FROM
  default.gold_analytics_orders
WHERE
  order_status = 'delivered'
GROUP BY
  customer_state
ORDER BY
  valor_total_vendas DESC
LIMIT 10
