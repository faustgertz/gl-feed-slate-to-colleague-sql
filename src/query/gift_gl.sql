WITH
  donor             AS
    (
      SELECT
        p.id         AS donor_id,
        p.[identity] AS donor_identity,
        (
          SELECT TOP 1 _f.value
          FROM field AS _f
          WHERE
              p.id = _f.record
          AND _f.field = 'legacy_id'
        )            AS donor_legacy_id,
        p.name       AS donor_name,
        'person'     AS donor_scope
      FROM person AS p
      UNION
      SELECT
        c.id         AS donor_id,
        c.[identity] AS donor_identity,
        (
          SELECT TOP 1 _f.value
          FROM field AS _f
          WHERE
              c.id = _f.record
          AND _f.field = 'company_legacy_id'
        )            AS donor_legacy_id,
        c.name       AS donor_name,
        'company'    AS donor_scope
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
  gifts             AS
    (
      SELECT
        g.id          AS gift_id,
        g.[identity]  AS gift_identity,
        TRY_CAST(
            TRY_CAST(g.amount AS DECIMAL(18, 2)) * 100
          AS INT
        )             AS gift_amount,
        (
          SELECT MAX(d)
          FROM (
                 VALUES
                   (
                     g.created
                   ),
                   (
                     pt.date
                   )
               ) AS VALUE(d)
        )             AS gift_settlement_post_datetime,
        COALESCE((
                   SELECT TOP 1 TRY_CAST(_f.[index] AS INT)
                   FROM field AS _f
                   WHERE
                       g.id = _f.record
                   AND _f.field = 'posted_to_gl'
                 ), 0
        )             AS gift_posted_to_gl,
        g.record      AS gift_record,
        p.[identity]  AS payment_identity,
        TRY_CAST(
            TRY_CAST(p.fee AS DECIMAL(18, 2)) * 100
          AS INT
        )             AS payment_fee,
        TRY_CAST(
            TRY_CAST(p.net AS DECIMAL(18, 2)) * 100
          AS INT
        )             AS payment_net,
        pt.[identity] AS settlement_identity,
        (
          SELECT TOP 1 _f.value
          FROM field AS _f
          WHERE
              g.fund = _f.record
          AND _f.field = 'fund_donation_gl'
        )             AS gift_credit_fund_donation_gl,
        COALESCE(
            (
              SELECT TOP 1 _f.value
              FROM field AS _f
              WHERE
                  g.id = _f.record
              AND _f.field = 'gift_gl_override'
            ),
            (
              SELECT TOP 1 _lp.export
              FROM [lookup.prompt] AS _lp
              WHERE
                  _lp.[key] = 'payment_account'
              AND p.payment_account = _lp.[value]
              AND _lp.active = 1
            ),
            CONCAT(
                (
                  SELECT TOP 1 _f.value
                  FROM field AS _f
                  WHERE
                      g.fund = _f.record
                  AND _f.field = 'fund_gl_fund'
                ),
                (
                  SELECT TOP 1 _f.value
                  FROM field AS _f
                  WHERE
                      g.fund = _f.record
                  AND _f.field = 'fund_gl_function'
                ),
                (
                  SELECT TOP 1 _lp.export2
                  FROM [lookup.prompt] AS _lp
                  WHERE
                    g.type = _lp.id
                ),
                (
                  SELECT TOP 1 _lp.export
                  FROM [lookup.prompt] AS _lp
                  WHERE
                    g.type = _lp.id
                )
            )
        )             AS gift_debit_gl,
        lp.export     AS payment_credit_gl,
        lp.export2    AS payment_debit_gl,
        lp.export3    AS payment_fee_gl,
        CONCAT(FORMAT(g.date, 'MMdd', 'en-US'), ' ',
               (
                 SELECT TOP 1 _f.value
                 FROM field AS _f
                 WHERE
                     g.fund = _f.record
                 AND _f.field = 'fund_name'
               ))     AS credit_summary_description,
        CONCAT(g.[identity], ' ',
               FORMAT(g.date, 'MMdd', 'en-US'), ' ',
               (
                 SELECT TOP 1 _f.value
                 FROM field AS _f
                 WHERE
                     g.fund = _f.record
                 AND _f.field = 'fund_name'
               ))     AS credit_detailed_description,
        CONCAT(FORMAT(g.date, 'MMdd', 'en-US'), ' ',
               (
                 SELECT TOP 1 COALESCE(_lp.short, _lp.value)
                 FROM [lookup.prompt] AS _lp
                 WHERE
                   g.type = _lp.id
               ))     AS debit_summary_description,
        CONCAT(g.[identity], ' ',
               FORMAT(g.date, 'MMdd', 'en-US'), ' ',
               (
                 SELECT TOP 1 COALESCE(_lp.short, _lp.value)
                 FROM [lookup.prompt] AS _lp
                 WHERE
                   g.type = _lp.id
               ))     AS debit_detailed_description
      FROM gift                      AS g
        LEFT JOIN payment            AS p
            ON g.payment = p.id
        LEFT JOIN [payment.transfer] AS pt
            ON p.transfer = pt.id
        LEFT JOIN [lookup.prompt]    AS lp
            ON lp.[key] = 'payment_account'
            AND p.payment_account = lp.[value]
            AND lp.active = 1
        LEFT JOIN donor              AS d
            ON g.record = d.donor_id
      WHERE
        /* Hard Credit Type */
        g.related IS NULL
      AND
        /* Gift Status is Received */
        /* The query runs faster if EXISTS is replaced with the
           [lookup.prompt].id (UUID) for 'Received'.
           g.status = '[lookup.prompt].id (UUID)' */
        EXISTS (
                 SELECT NULL
                 FROM [lookup.prompt] AS _lp
                 WHERE
                     g.status = _lp.id
                 AND _lp.value = 'Received'
               )
      AND
        /* Settlement Status is Paid or Payment Doesn't Exist */
        (
          /* Settlement Status is Paid */
          (
            /* Payment account is for Donations */
            /* The query runs faster if EXISTS is replaced with the
               [lookup.prompt].id (UUID) the account name.
               p.slate_payment = '[lookup.prompt].id (UUID)' */
            EXISTS (
                     SELECT NULL
                     FROM [lookup.payment] AS _lp
                     WHERE
                         p.slate_payment = _lp.id
                         -- Replace 'Donations Account' with correct value
                     AND _lp.name = 'Donations Account'
                   )
              AND
              /* Settlement Status is Paid */
            pt.status = 'Paid'
              AND
              /* Neither a test payment nor settlement */
            'true' NOT IN (
                           p.test,
                           pt.test
              )
            --AND
            /* Only Received or Refunded transactions. */
            /* Rejected ACH transaction fees still needs to be
               figured out. */
            --  payment.action IN ('Received', 'Refunded')
            )
            OR
            /* Payment Doesn't Exist */
          p.id IS NULL
          )
      AND
        /* Gift not made by a test person or company */
        NOT EXISTS (
                     SELECT NULL
                     FROM tag AS _t
                     WHERE
                         p.record = _t.record
                     AND _t.tag IN (
                                    'test',
                                    'companies_test'
                       )
                   )
    ),
  credits           AS
    (
      SELECT
        CONCAT(
            gift_identity, '-',
            'Credit', '-',
            'Amount'
        )                            AS id,
        gift_id,
        gift_settlement_post_datetime,
        gift_posted_to_gl,
        gift_record,
        payment_identity,
        settlement_identity,
        gift_credit_fund_donation_gl AS gl_account_#,
        0                            AS debit,
        gift_amount                  AS credit,
        credit_summary_description   AS summary_description,
        credit_detailed_description  AS detailed_description
      FROM gifts
    ),
  debits            AS
    (
      SELECT
        CONCAT(
            gift_identity, '-',
            'Debit', '-',
            'Amount'
        )                          AS id,
        gift_id,
        gift_settlement_post_datetime,
        gift_posted_to_gl,
        gift_record,
        payment_identity,
        settlement_identity,
        gift_debit_gl              AS gl_account_#,
        gift_amount                AS debit,
        0                          AS credit,
        debit_summary_description  AS summary_description,
        debit_detailed_description AS detailed_description
      FROM gifts
      WHERE
          payment_net IS NULL
      AND payment_fee IS NULL
      UNION
      SELECT
        CONCAT(
            gift_identity, '-',
            'Debit', '-',
            'Net'
        )                                          AS id,
        gift_id,
        gift_settlement_post_datetime,
        gift_posted_to_gl,
        gift_record,
        payment_identity,
        settlement_identity,
        gift_debit_gl                              AS gl_account_#,
        payment_net                                AS debit,
        0                                          AS credit,
        CONCAT(debit_summary_description, ' Net')  AS summary_description,
        CONCAT(debit_detailed_description, ' Net') AS detailed_description
      FROM gifts
      WHERE
        payment_net IS NOT NULL
      AND
        /* Only reference one (the most recent?) gift associated with
           the payment identity */
        NOT EXISTS (
                     SELECT NULL
                     FROM gifts AS _g
                     WHERE
                         gifts.payment_identity = _g.payment_identity
                     AND gifts.gift_identity < _g.gift_identity
                   )
      UNION
      SELECT
        CONCAT(
            gift_identity, '-',
            'Debit', '-',
            'Fee'
        )                                          AS id,
        gift_id,
        gift_settlement_post_datetime,
        gift_posted_to_gl,
        gift_record,
        payment_identity,
        settlement_identity,
        payment_fee_gl                             AS gl_account_#,
        payment_fee                                AS debit,
        0                                          AS credit,
        CONCAT(debit_summary_description, ' Fee')  AS summary_description,
        CONCAT(debit_detailed_description, ' Fee') AS detailed_description
      FROM gifts
      WHERE
        payment_fee IS NOT NULL
      AND
        /* Only reference one (the most recent?) gift associated with
           the payment identity */
        NOT EXISTS (
                     SELECT NULL
                     FROM gifts AS _g
                     WHERE
                         gifts.payment_identity = _g.payment_identity
                     AND gifts.gift_identity < _g.gift_identity
                   )
    ),
  stripe_debits     AS
    (
      SELECT
        CONCAT(
            p.[identity], '-',
            'Debit', '-',
            'Stripe Fee'
        )                 AS id,
        NULL              AS gift_id,
        pt.date           AS gift_settlement_post_datetime,
        NULL              AS gift_posted_to_gl,
        NULL              AS gift_record,
        p.[identity]      AS payment_identity,
        pt.[identity]     AS settlement_identity,
        -- Replace with correct GL #
        '111111111111111' AS gl_account_#,
        TRY_CAST(
            TRY_CAST(p.amount AS DECIMAL(18, 2)) * 100
          AS INT
        ) * -1            AS debit,
        0                 AS credit,
        CONCAT(
            FORMAT(pt.date, 'MMdd', 'en-US'),
            ' Stripe Fee'
        )                 AS summary_description,
        CONCAT(
            p.[identity], ' ',
            FORMAT(pt.date, 'MMdd', 'en-US'),
            ' Stripe Fee'
        )                 AS detailed_description
      FROM payment                    AS p
        INNER JOIN [payment.transfer] AS pt
            ON p.transfer = pt.id
      WHERE
        p.payment_account = 'Slate Payments'
      AND
        /* Settlement Date */
        (
          p.slate_payment IS NOT NULL
            AND
            /* We don't want to see any settlements before this */
          CONVERT(DATE, pt.date) > '5-1-2023'
          )
    ),
  stripe_credits    AS
    (
      SELECT
        CONCAT(
            payment_identity, '-',
            'Credit', '-',
            'Stripe Fee'
        )                 AS id,
        gift_id,
        gift_settlement_post_datetime,
        gift_posted_to_gl,
        gift_record,
        payment_identity,
        settlement_identity,
        -- Replace with correct GL #
        '000000000000000' AS gl_account_#,
        0                 AS debit,
        debit             AS credit,
        summary_description,
        detailed_description
      FROM stripe_debits
    ),
  gl_lines_combined AS
    (
      SELECT
        id,
        gift_id,
        gift_settlement_post_datetime,
        gift_posted_to_gl,
        gift_record,
        payment_identity,
        settlement_identity,
        gl_account_#,
        debit,
        credit,
        summary_description,
        detailed_description
      FROM debits
      UNION
      SELECT
        id,
        gift_id,
        gift_settlement_post_datetime,
        gift_posted_to_gl,
        gift_record,
        payment_identity,
        settlement_identity,
        gl_account_#,
        debit,
        credit,
        summary_description,
        detailed_description
      FROM credits
      UNION
      SELECT
        id,
        gift_id,
        gift_settlement_post_datetime,
        gift_posted_to_gl,
        gift_record,
        payment_identity,
        settlement_identity,
        gl_account_#,
        debit,
        credit,
        summary_description,
        detailed_description
      FROM stripe_credits
      UNION
      SELECT
        id,
        gift_id,
        gift_settlement_post_datetime,
        gift_posted_to_gl,
        gift_record,
        payment_identity,
        settlement_identity,
        gl_account_#,
        debit,
        credit,
        summary_description,
        detailed_description
      FROM stripe_debits
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
        glc.id,
        glc.gift_id,
        g.[identity]                           AS gift_identity,
        g.amount                               AS gift_amount,
        (
          SELECT TOP 1 COALESCE(_lp.short, _lp.value)
          FROM [lookup.prompt] AS _lp
          WHERE
            g.type = _lp.id
        )                                      AS gift_type,
        g.date                                 AS gift_datetime,
        g.created                              AS gift_created,
        g.updated                              AS gift_updated,
        glc.gift_settlement_post_datetime,
        glc.gift_posted_to_gl,
        d.donor_identity,
        d.donor_legacy_id,
        d.donor_name,
        d.donor_scope,
        glc.payment_identity,
        p.amount                               AS payment_amount,
        p.net                                  AS payment_net,
        p.fee                                  AS payment_fee,
        p.added_fee                            AS payment_added_fee,
        p.date                                 AS payment_datetime,
        p.action                               AS payment_action,
        p.payment_account,
        p.payment_provider,
        p.payment_type,
        p.payment_type_detail,
        p.last4                                AS payment_last4,
        glc.settlement_identity,
        pt.date                                AS settlement_datetime,
        pt.status                              AS settlement_status,
        pt.target_bank                         AS settlement_target_bank,
        pt.account_last4                       AS settlement_account_last4,
        glc.summary_description,
        glc.detailed_description
      FROM gl_lines_combined         AS glc
        LEFT JOIN gift               AS g
            ON glc.gift_id = g.id
        LEFT JOIN payment            AS p
            ON glc.payment_identity = p.[identity]
        LEFT JOIN [payment.transfer] AS pt
            ON glc.settlement_identity = pt.[identity]
        LEFT JOIN donor              AS d
            ON glc.gift_record = d.donor_id
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
FROM [gl_detailed_report]
WHERE
  gift_settlement_post_datetime BETWEEN '2024/02/01' AND '2024/02/05'
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
  gift_settlement_post_datetime BETWEEN '2024/02/01' AND '2024/02/05';
 */
