#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# PROJECT NAME/FOLDER: MESEC/R
# FILENAME: seifa_agg.R
# AUTHOR: Nhung Seidensticker
# R VERSION: 4.0.2
# DATE CREATED: 03.11.2021
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#' Get the particular SEIFA index variable for the selected clue blocks
#'
#' @details
#' **seifaindex** abbreviations:
#' * IRSD - Index of Relative Socio-economic Disadvantage
#' * IEO - Index of Education and Occupation
#' * IER - Index of Economic Resources
#' * IRSAD - Index of Relative Socio-economic Advantage and Disadvantage
#'
#' **measure** abbreviations:
#' * SCORE - Score
#' * RWAD - Rank within Australia - Decile
#' * RWSD - Rank within State or Territory - Decile
#' * RWSP - Rank within State or Territory - Percentile
#' * RWSR - Rank within State or Territory
#' * RWAR - Rank within Australia
#' * RWAP - Rank within Australia - Percentile
#' * URP - Usual resident population
#' @md
#'
#' @param con The datbase connection
#' @param clue_blocks The CLUE blocks selected
#' @param abs_table The table in the abs schema
#' @param seifaindex The selected seifaindextype
#' @param measure The selected seifa_measure
#' @param sel_year Select the year you want to return data for.
#' The only years available are 2011 and 2016 currently. 
#'
#' @import DBI
#' @importFrom glue glue_sql
#'
#' @return A data frame with the seifa estimate for the parameters entered.
#'
#' @export
#' @examples
#' \dontrun{
#' CON <- dbCon() # your database connection
#'
#' test_cb <- c('73','74','83','84')
#' # test_cb <- c(11:18, 21:28) # this will be converted to character
#' seifa_agg(CON, test_cb, 'sa1_seifa', 'IRSD', 'SCORE', 2016)
#' }
#'
seifa_agg <- function(con, clue_blocks, abs_table = 'sa1_seifa',
                       seifaindex = 'IRSD', measure = 'SCORE', sel_year = 2016){
  # checks
  if (!seifaindex %in% c('IRSD', 'IEO', 'IER', 'IRSAD')){
    rlang::abort("'seifaindex' should be one of 'IRSD', 'IEO', 'IER', 'IRSAD'.
                 See documentation for details.")
  }
  
  if(!sel_year %in% c('2011', '2016')){
    rlang::abort("'Years supported 2011 and 2016'")
  }

  if (!measure %in% c('SCORE', 'RWAD', 'RWSD', 'RWSP', 'RWSR', 'RWAR', 'RWAP', 'URP')){
    rlang::abort("'measure' should be one of: 'SCORE', 'RWAD',
    'RWSD', 'RWSP', 'RWSR', 'RWAR', 'RWAP', 'URP'.
                 See documentation for details.")
  }

  # convert clue_blocks to character if they aren't
  clue_blocks <- as.character(clue_blocks)

  seifa_sql <- glue::glue_sql("WITH SEIFA as (SELECT DISTINCT seifa.sa1_main,
                                             cc.sa1_persons,
                                             seifa.seifaindextype,
                                             seifa.seifa_measure,
                                             seifa.value seifa_index,
                                             cc.year,
                                             (seifa.value * cc.sa1_persons) weight
                                             FROM abs.{`abs_table`} seifa
                                             INNER JOIN (SELECT DISTINCT sa1_main,
                                                         sa1_persons,
                                                         year
                                                         FROM concord.cbsa1mb
                                                         WHERE block_id IN ({clueblocks*})
                                                         AND year IN ({selyear*})) cc
                                             ON cc.sa1_main = seifa.sa1_main
                                             WHERE seifaindextype IN ({sel_index*})
                                             AND seifa_measure IN ({sel_measure*})
                                             AND seifa.value NOTNULL)
                                 SELECT seifaindextype,
                                        seifa_measure,
                                        round(sum(weight)/sum(sa1_persons)) as value,
                                        year
                                 FROM SEIFA
                                 GROUP BY seifaindextype, seifa_measure, year;",
                               clueblocks = clue_blocks,
                               sel_index = seifaindex,
                               sel_measure = measure,
                               selyear = sel_year,
                               .con = con)


  abs_query <- DBI::dbSendQuery(con, seifa_sql)

  results <- DBI::dbFetch(abs_query, n = -1)

  return(results)

  DBI::dbClearResult(abs_query)

}

