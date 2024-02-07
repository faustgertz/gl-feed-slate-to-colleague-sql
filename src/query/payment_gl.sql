WITH
  customer          AS
    (
      SELECT
        p.id         AS customer_id,
        p.[identity] AS customer_identity,
        (
          SELECT TOP 1 _f.value
          FROM field AS _f
          WHERE
              p.id = _f.record
          AND _f.field = 'legacy_id'
        )            AS customer_legacy_id,
        p.name       AS customer_name,
        'person'     AS customer_scope
      FROM person AS p
      UNION
      SELECT
        c.id         AS customer_id,
        c.[identity] AS customer_identity,
        (
          SELECT TOP 1 _f.value
          FROM field AS _f
          WHERE
              c.id = _f.record
          AND _f.field = 'company_legacy_id'
        )            AS customer_legacy_id,
        c.name       AS customer_name,
        'company'    AS customer_scope
      FROM [dataset.row] AS c
      WHERE
        EXISTS (
                 SELECT NULL
                 FROM dataset AS _d
                 WHERE
                     c.dataset = _d.id
                 AND _d.type LIKE '%giving%'
               )
    ),
  settled_payments  AS
    (
      SELECT
        p.[identity]  AS payment_identity,
        p.date        AS payment_datetime,
        TRY_CAST(
            TRY_CAST(p.amount AS DECIMAL(18, 2)) * 100
          AS INT
        )             AS payment_amount,
        TRY_CAST(
            TRY_CAST(p.fee AS DECIMAL(18, 2)) * 100
          AS INT
        )             AS payment_fee,
        TRY_CAST(
            TRY_CAST(p.net AS DECIMAL(18, 2)) * 100
          AS INT
        )             AS payment_net,
        p.payment_account,
        p.action      AS payment_action,
        p.record      AS payment_record,
        pt.[identity] AS settlement_identity,
        pt.date       AS settlement_datetime,
        lp.export     AS credit_gl,
        lp.export2    AS debit_gl,
        lp.export3    AS fee_gl
      FROM payment                    AS p
        INNER JOIN [payment.transfer] AS pt
            ON p.transfer = pt.id
        INNER JOIN [lookup.prompt]    AS lp
            ON lp.[key] = 'payment_account'
            AND p.payment_account = lp.[value]
            AND lp.active = 1
      WHERE
        /* Account != Donations */
        NOT EXISTS (
                     SELECT NULL
                     FROM [lookup.payment] AS _lp
                     WHERE
                         p.slate_payment = _lp.id
                     AND _lp.name = 'Donations Account'
                   )
      AND
        /* Settlement Status IS Paid */
        pt.status = 'Paid'
      AND
        /* Not a test payment or settlement */
        'true' NOT IN (p.test, pt.test)
        --AND
        /* Only Received or Refunded transactions. */
        /* Rejected ACH transaction fees still needs to be
           figured out. */
        --action IN ('Received', 'Refunded')
      AND
        /* Not made by a test person or company */
        NOT EXISTS(
                    SELECT NULL
                    FROM tag AS _t
                    WHERE
                        p.record = _t.record
                    AND _t.tag IN ('test', 'companies_test')
                  )
    ),
  credits           AS
    (
      SELECT
        CONCAT(
            payment_identity, '-',
            settlement_identity, '-',
            'Credit', '-',
            payment_action, '-',
            'Amount'
        )         AS id,
        payment_identity,
        payment_record,
        settlement_identity,
        settlement_datetime,
        credit_gl AS gl_account_#,
        CASE payment_action
          WHEN 'Received'
            THEN payment_amount
            ELSE 0
        END       AS credit,
        CASE payment_action
          WHEN 'Refunded'
            THEN payment_amount
            ELSE 0
        END       AS debit,
        CONCAT(
            FORMAT(payment_datetime, 'MMdd', 'en-US'), ' ',
            payment_account
        )         AS summary_description,
        CONCAT(
            payment_identity, ' ',
            FORMAT(payment_datetime, 'MMdd', 'en-US'), ' ',
            payment_account
        )         AS detailed_description
      FROM settled_payments
    ),
  debits            AS
    (
      SELECT
        CONCAT(
            payment_identity, '-',
            settlement_identity, '-',
            'Debit', '-',
            payment_action, '-',
            'Net') AS id,
        payment_identity,
        payment_record,
        settlement_identity,
        settlement_datetime,
        debit_gl   AS gl_account_#,
        CASE payment_action
          WHEN 'Received'
            THEN ABS(payment_net)
            ELSE 0
        END        AS debit,
        CASE payment_action
          WHEN 'Received'
            THEN 0
            ELSE ABS(payment_net)
        END        AS credit,
        CONCAT(
            FORMAT(payment_datetime, 'MMdd', 'en-US'),
            ' Net'
        )          AS summary_description,
        CONCAT(payment_identity, ' ',
               FORMAT(payment_datetime, 'MMdd', 'en-US'),
               ' Net'
        )          AS detailed_description
      FROM settled_payments
      UNION
      SELECT
        CONCAT(
            payment_identity, '-',
            settlement_identity, '-',
            'Debit', '-',
            payment_action, '-',
            'Fee') AS id,
        payment_identity,
        payment_record,
        settlement_identity,
        settlement_datetime,
        fee_gl     AS gl_account_#,
        CASE payment_action
          WHEN 'Refunded'
            THEN 0
            ELSE payment_fee
        END        AS debit,
        CASE payment_action
          WHEN 'Refunded'
            THEN payment_fee
            ELSE 0
        END        AS credit,
        CONCAT(
            FORMAT(payment_datetime, 'MMdd', 'en-US'),
            ' Fee'
        )          AS summary_description,
        CONCAT(
            payment_identity, ' ',
            FORMAT(payment_datetime, 'MMdd', 'en-US'),
            ' Fee'
        )          AS detailed_description
      FROM settled_payments
    ),
  gl_lines_combined AS
    (
      SELECT
        id,
        payment_identity,
        payment_record,
        settlement_identity,
        settlement_datetime,
        gl_account_#,
        debit,
        credit,
        summary_description,
        detailed_description
      FROM debits
      UNION
      SELECT
        id,
        payment_identity,
        payment_record,
        settlement_identity,
        settlement_datetime,
        gl_account_#,
        debit,
        credit,
        summary_description,
        detailed_description
      FROM credits
    ),
  gl_detailed_report AS
    (
      SELECT
        'JE'                                   AS [Source],
        CONCAT(
            'SL',
            FORMAT(GETDATE(), 'MMddyy', 'en-US')
        )                                      AS Reference,
        glc.gl_account_#                       AS [GL account #],
        glc.debit                              AS Debit,
        glc.credit                             AS Credit,
        glc.detailed_description               AS [Description],
        FORMAT(GETDATE(), 'MM/dd/yy', 'en-US') AS [Trans Date],
        NULL                                   AS [Project Number],
        glc.payment_identity,
        p.date                                 AS payment_datetime,
        p.amount                               AS payment_amount,
        p.fee                                  AS payment_fee,
        p.net                                  AS payment_net,
        p.added_fee                            AS payment_added_fee,
        p.payment_provider,
        p.payment_account,
        p.action                               AS payment_action,
        p.payment_type,
        p.payment_type_detail,
        p.last4                                AS payment_last4,
        p.activity                             AS payment_activity,
        glc.settlement_identity,
        glc.settlement_datetime,
        pt.status                              AS settlement_status,
        pt.target_bank                         AS settlement_target_bank,
        pt.account_last4                       AS settlement_account_last4,
        c.customer_identity,
        c.customer_legacy_id,
        c.customer_name,
        c.customer_scope,
        glc.summary_description,
        glc.detailed_description
      FROM gl_lines_combined         AS glc
        LEFT JOIN payment            AS p
            ON glc.payment_identity = p.[identity]
        LEFT JOIN [payment.transfer] AS pt
            ON glc.settlement_identity = pt.[identity]
        LEFT JOIN customer           AS c
            ON glc.payment_record = c.customer_id
    )
SELECT
  [Source],
  Reference,
  [GL account #],
  SUM(Debit)          AS Debit,
  SUM(Credit)         AS Credit,
  summary_description AS [Description],
  [Trans Date],
  [Project Number]
FROM gl_detailed_report
WHERE
  settlement_datetime BETWEEN '2024/01/01' AND '2024/02/05'
GROUP BY [Source],
         Reference,
         [GL account #],
         summary_description,
         [Trans Date],
         [Project Number]
ORDER BY LEFT(summary_description, 4),
         Credit              DESC,
         summary_description DESC;
GO

/*
SELECT *
FROM gl_detailed_report
WHERE
  settlement_datetime BETWEEN '2024/01/01' AND '2024/02/05';
*/
