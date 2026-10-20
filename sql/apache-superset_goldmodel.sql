SELECT 
    f.invoice_number,
    f.quantity,
    f.rate,
    f.value,
    f.lc,
    f.reference_no,
    c.customer_code,
    c.customer_name, 
    i.item_code,
    i."group" AS item_group, 
    i.description,
    u.unit,
    d.date,
    d.year,
    d.month,
    d.quarter
FROM fact_sales f
LEFT JOIN dim_customer c ON f.customer_code = c.customer_code
LEFT JOIN dim_item i ON f.item_code = i.item_code
LEFT JOIN dim_date d ON f.date = d.date
LEFT JOIN dim_unit u ON f.unit = u.unit
