SELECT I.ID_INVOICE, I.NUM_CLIENT, I.HEURE, I.TOTAL, L.PRICE, L.QUANTITE
FROM INVOICE I
         JOIN LIST L on I.ID_INVOICE = L.ID_INVOICE
WHERE HEURE BETWEEN '2022-01-01 00:00:00' AND '2022-12-31 23:59:59'