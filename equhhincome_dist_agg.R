#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# PROJECT NAME/FOLDER: MESEC/R
# FILENAME: equhhincome_dist_agg.R
# AUTHOR: Nhung Seidensticker
# R VERSION: 4.0.2
# DATE CREATED: 02.12.2021
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#' Gets counts of income by weekly and yearly income. This is a calculated equivilised income.
#' Equivalised income is a measure of household income that takes account of the differences in
#' a household's size and composition, and thus is equivalised or made equivalent for all household sizes and compositions.
#' the table uses dwellings
#'
#' @param con The datbase connection
#' @param clue_blocks The CLUE blocks selected
#' @param abs_table The table in the schema
#' @param sel_year Select the year you want to return data for.
#' The only years available are 2011 and 2016 currently. 
#'
#' @import DBI
#' @import rlang
#' @importFrom glue glue_sql
#' @importFrom magrittr %>%
#'
#' @return a data frame with counts of equivalised dwelling income based on distribution range. 
#' The return value is the estimated equivalised dwelling income. 
#'
#' @family income
#'
#' @export
#' @examples
#' \dontrun{
#' CON <- dbCon() # your database connection
#'
#' test_cb <- c('73','74','83','84')
#' # test_cb <- c(11:18, 21:28) # this will be converted to character
#' equhhincome_dist_agg(CON, test_cb, 'sa1_tb_hh_equivalised_income', 2016)
#' }

equhhincome_dist_agg <- function(con, clue_blocks, 
                            abs_table = 'sa1_tb_hh_equivalised_income', sel_year = 2016) {

  #checks for year selection
  if(!sel_year %in% c('2011', '2016')){
    rlang::abort("'Years supported 2011 and 2016'")
  }
  
  # convert clue_blocks to character if they aren't
  clue_blocks <- as.character(clue_blocks)

  mb_sql <- glue::glue_sql("WITH INCOME AS (
                        SELECT eq_income_yearly,
                               eq_income_weekly, 
                               value * per_pct as dwellings,
                               cc.year
                      FROM concord.cbsa1mb cc
                      INNER JOIN abs.{`abs_table`} g01
                      ON cc.sa1_7dig = g01.sa1_7dig AND cc.year = g01.year
                      WHERE cc.block_id IN ({clueblocks*})
                      AND cc.year IN ({selyear*}))

                    SELECT eq_income_weekly,  
                           eq_income_yearly,
                           round(sum(dwellings)) as value,
                           year
                    FROM INCOME
                    GROUP BY eq_income_yearly, eq_income_weekly, year
                        ORDER BY CASE 
                              WHEN eq_income_weekly = 'Negative income' THEN 1
                              WHEN eq_income_weekly = 'Nil income' THEN 2
                              WHEN eq_income_weekly = '1-149' OR eq_income_weekly = '1-199' THEN 3
                              WHEN eq_income_weekly = '150-299' OR eq_income_weekly = '200-299' THEN 4
                              WHEN eq_income_weekly = '300-399' OR eq_income_weekly = '300-399' THEN 5
                              WHEN eq_income_weekly = '400-499' OR eq_income_weekly = '400-599' THEN 6
                              WHEN eq_income_weekly = '500-649' OR eq_income_weekly = '600-799' THEN 7
                              WHEN eq_income_weekly = '650-799' OR eq_income_weekly = '800-999' THEN 8
                              WHEN eq_income_weekly = '800-999' OR eq_income_weekly = '1,000-1,249' THEN 9
                              WHEN eq_income_weekly = '1,000-1,249' OR eq_income_weekly ='1,250-1,499' THEN 10
                              WHEN eq_income_weekly = '1,250-1,499' OR eq_income_weekly = '1,500-1,999' THEN 11
                              WHEN eq_income_weekly = '1,500-1,749' OR eq_income_weekly = '2,000 or more' THEN 12
                              WHEN eq_income_weekly = '1,750-1,999' THEN 13
                              WHEN eq_income_weekly = '2,000-2,499' THEN 14
                              WHEN eq_income_weekly = '2,500-2,999' THEN 15
                              WHEN eq_income_weekly = '3,000 or more' THEN 16
                              WHEN eq_income_weekly = 'Not stated' THEN 17
                              WHEN eq_income_weekly = 'Partial income stated' THEN 18
                              WHEN eq_income_weekly = 'All incomes not stated' THEN 19
                              WHEN eq_income_weekly = 'Not applicable' THEN 20
                           END,
                           year;",
                           clueblocks = clue_blocks,
                           selyear = sel_year,
                           .con = con)


  abs_query <- DBI::dbSendQuery(con, mb_sql)
  results <- DBI::dbFetch(abs_query, n = -1)
  return(results)


  DBI::dbClearResult(abs_query)

}
