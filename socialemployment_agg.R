#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# PROJECT NAME/FOLDER: MESEC/R
# FILENAME: socialemployment_agg.R
# AUTHOR: Nhung Seidensticker
# R VERSION: 4.0.2
# DATE CREATED: 14.12.2021
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#' Gets the information for the respective SA2 or Postcode where the given clue blocks fall within.
#'
#' Best to use contiguous groups of CLUE blocks rather than single ones.
#'
#' #' @details
#' **Supported tables**
#'
#' * payment_types_qrt_pc
#' * payment_types_qrt_sa2
#' * sa2_salm
#' * jobkeeper_monthly_applications_pc
#' * jobseeker_montly_payments_sa2
#'
#' ** Returns **
#'
#' The result will either return suburb, SA2 or postcode depending on the table selected.
#' suburb: The suburb, which seems to be an amalgamation of postcodes in some instances. For example, North Melbourne - West Melbourne is one area.
#'
#' *Corresponding attributes*
#'
#' |Table|Time series|attributes|
#' |---|---|---|
#' |payment_types_qrt_sa2 | quarter: from March 2015 to current| DHHS payment types|
#' |payment_types_qrt_pc | quarter: from March 2015 to current| DHHS payment types|
#' |sa2_salm |quarter: from March 2011 to current |umemployment (persons or %), labour force|
#' |jobkeeper_monthly_applications_pc|monthly: from March 2020 to current|payment type, number persons|
#' |jobseeker_montly_payments_sa2|Monthly: from April 2020 - March 2021|number of applicants|
#' @md
#'
#' @param con The datbase connection
#' @param clue_blocks The CLUE blocks selected
#' @param sel_tbl The table in the dhhs schema.
#'
#' @import DBI
#' @import rlang
#' @importFrom glue glue_sql
#' @importFrom magrittr %>%
#'
#' @return a data frame that returns the information for table area where clue blocks fall within. This does not aggregate
#' the data. It only returns information on the respective SA2, LGA or Postcode where the clue block falls within.
#'
#' @export
#' @examples
#' \dontrun{
#' CON <- dbCon() # your database connection
#'
#' test_cb <- c('73','74','83','84')
#' # test_cb <- c(11:18, 21:28) # this will be converted to character
#' socialemployment_agg(CON, test_cb, 'jobseeker_monthly_payments_sa2')
#' }

socialemployment_agg <- function(con, clue_blocks, sel_tbl = 'jobseeker_monthly_payments_sa2'){


  #convert clue_blocks to character if they aren't
  clue_blocks <- as.character(clue_blocks)

  jobseeker_sql <- glue::glue_sql("WITH JS AS (
                        SELECT distinct dss.sa2_5dig,
                               cc.sa2_main,
                               cc.sa2_name,
                               to_date(month, 'Month YYYY') as month,
                               series,
                               value
                        FROM concord.cbsa2 cc
                        INNER JOIN dss.{`sel_tbl`} dss
                        ON cc.sa2_5dig = dss.sa2_5dig
                        WHERE cc.block_id IN ({clueblocks*})
                        )

                        SELECT sa2_main,
                               sa2_name,
                               month,
                               series,
                               value 
                        FROM JS
                        ORDER BY month, series, sa2_name ASC;",
                           clueblocks = clue_blocks,
                           .con = con)

  jobkeeker_sql <- glue::glue_sql("WITH JK AS(
                        SELECT distinct dss.postcode,
                               to_date(month, 'Month YYYY') as month,
                               applications
                        FROM concord.cbpostcodes cc
                        INNER JOIN dss.{`sel_tbl`} dss
                        ON cc.abs_postcode = dss.postcode
                        WHERE cc.block_id IN ({clueblocks*})
                        )

                        SELECT postcode,
                               month,
                               applications as value
                        FROM JK
                        ORDER BY month, postcode ASC;",
                                  clueblocks = clue_blocks,
                                  .con = con)

  payments_qrt_pc <- glue::glue_sql("WITH PAYTYPE AS(
                        SELECT distinct dss.postcode,
                               to_date(quarter, 'Month YYYY') as quarter,
                               series,
                               value
                        FROM concord.cbpostcodes cc
                        INNER JOIN dss.{`sel_tbl`} dss
                        ON cc.abs_postcode = dss.postcode
                        WHERE cc.block_id IN ({clueblocks*})
                        )

                        SELECT postcode,
                               quarter,
                               series,
                               value
                        FROM PAYTYPE
                        ORDER BY quarter ASC, series, postcode;",
                                    clueblocks = clue_blocks,
                                    .con = con)

  payments_qrt_sa2 <- glue::glue_sql("WITH PAYTYPE AS(
                        SELECT distinct dss.sa2_5dig,
                                 cc.sa2_main,
                                 cc.sa2_name,
                                 to_date(quarter, 'Month YYYY') as quarter,
                                 series,
                                 value
                        FROM concord.cbsa2 cc
                        INNER JOIN dss.{`sel_tbl`} dss
                        ON cc.sa2_5dig = dss.sa2_5dig
                        WHERE cc.block_id IN ({clueblocks*})
                        )

                        SELECT sa2_main,
                               sa2_name,
                               quarter,
                               series,
                               value
                        FROM PAYTYPE
                        ORDER BY quarter ASC, series, sa2_main;",
                                    clueblocks = clue_blocks,
                                    .con = con)

  salm_sa2 <- glue::glue_sql("WITH PAYTYPE AS(
                        SELECT distinct salm.sa2_main,
                               cc.sa2_name,
                               to_date(quarter, 'Month YYYY') as quarter,
                               series,
                               value
                        FROM concord.cbsa2 cc
                        INNER JOIN lmip.{`sel_tbl`} salm
                        ON cc.sa2_main = salm.sa2_main
                        WHERE cc.block_id IN ({clueblocks*})
                        )

                        SELECT sa2_main,
                               sa2_name,
                               quarter,
                               series,
                               value
                        FROM PAYTYPE
                        ORDER BY quarter, series, sa2_main ASC;",
                                     clueblocks = clue_blocks,
                                     .con = con)



  if (sel_tbl == 'jobseeker_monthly_payments_sa2'){
    table_query <- DBI::dbSendQuery(con, jobseeker_sql)
    results <- DBI::dbFetch(table_query, n = -1)
    return(results)
    DBI::dbClearResult(table_query)

  } else if (sel_tbl == 'jobkeeper_monthly_applications_pc'){
    table_query <- DBI::dbSendQuery(con, jobkeeker_sql)
    results <- DBI::dbFetch(table_query, n = -1)
    return(results)
    DBI::dbClearResult(table_query)

  } else if (sel_tbl == 'payment_types_qrt_pc'){
    table_query <- DBI::dbSendQuery(con, payments_qrt_pc)
    results <- DBI::dbFetch(table_query, n = -1)
    return(results)
    DBI::dbClearResult(table_query)

  } else if (sel_tbl == 'payment_types_qrt_sa2'){
    table_query <- DBI::dbSendQuery(con, payments_qrt_sa2)
    results <- DBI::dbFetch(table_query, n = -1)
    return(results)
    DBI::dbClearResult(table_query)

  } else if (sel_tbl == 'sa2_salm'){
    table_query <- DBI::dbSendQuery(con, salm_sa2)
    results <- DBI::dbFetch(table_query, n = -1)
    return(results)
    DBI::dbClearResult(table_query)

  } else {cat('Please use one of the following tables: \n jobseeker_monthly_payments_sa2 \n jobkeeper_monthly_applications_pc \n payment_types_qrt_pc \n payment_types_qrt_sa2 \n sa2_salm')
  }

}
