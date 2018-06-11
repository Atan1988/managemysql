#' update values based on data frame
#' @param con the RMySQL connection
#' @param tab the table name string
#' @param df the data frame to update values
#' @param val_fields the value fields to be updated
#' @param pk_fields the fields that locate unique rows
#' @export
UPDATE_value <- function(con, tab, df, val_fields, pk_fields) {
  val_fields <- paste0('`', val_fields, '`')
  pk_fields <-   paste0('`', pk_fields, '`')
  tmp_tab <- paste0(tab, stringi::stri_rand_strings(1, "10"))

  DBI::dbWriteTable(con, name = tmp_tab, value = df , append = F,
                    row.names = FALSE, temporary = T)

  upd_str <- paste0('UPDATE ', paste(c(tab, tmp_tab), collapse = ","), ' SET ',
                    paste(paste0('`', tab, '`.', val_fields, ' = `', tmp_tab, '`.',
                                 val_fields), collapse = ", "), ' WHERE ',
                    paste(paste0('`', tab, '`.', pk_fields, ' = `', tmp_tab, '`.',
                                 pk_fields), collapse = " and "), ';'
  )


  res <- DBI::dbSendQuery(con, upd_str)
  DBI::dbClearResult(res)

  droptables(con, tmp_tab)
}

#' insert values based on data frame
#' @param con the RMySQL connection
#' @param tab the table name string
#' @param df the data frame to update values
#' @export
insert_value <- function(pool, tab, df){
  con <- pool::poolCheckout(pool)
  res <- DBI::dbWriteTable(con, name = tab,
                           value = df, append = T, row.names = FALSE)
  pool::poolReturn(con)
  return(res)
}

#' get tables as data frame
#' @param con the RMySQL connection
#' @param tab the table name string
#' @export
get_table  <- function(pool, tab) {
  con <- pool::poolCheckout(pool)
  db <- dplyr::tbl(con, tab) %>% dplyr::collect()
  pool::poolReturn(con)
  return(db)
}
