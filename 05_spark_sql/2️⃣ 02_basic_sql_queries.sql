-- Seleccionar datos
SELECT *
FROM ventas
LIMIT 10;

-- Agregación
SELECT
    id_cliente,
    SUM(monto) AS total_ventas
FROM ventas
GROUP BY id_cliente;

-- Filtro
SELECT *
FROM ventas
WHERE monto > 1000;
