#'create a database
#'@param con the RMySQL connection
#'@param DBname the RMySQL Database Name
#'@export
create_database <- function(con, DBname) {
  SQLstr <- paste("CREATE DATABASE IF NOT EXISTS ", DBname)
  res <- RMySQL::dbSendQuery(con, SQLstr)
  DBI::dbClearResult(res)
}


#' drop tables on the connection
#' @param con the RMySQL connection
#' @param tab the table name string
#' @export
droptables <- function(con, tab) {
  sqlstrs <- paste0('DROP TABLE `', tab, '`;')

  sqlstrs %>% purrr::map( function(x) {
    res <- RMySQL::dbSendQuery(con, x)
    DBI::dbClearResult(res)
    })
}

#'Create table
#'@param con the RMySQL connection
#'@param tab the table name string
#'@param colnams the names of the columns
#'@param coltypes the types of the columns
#'@param keycols the colnums that will form indexes
#'@export
create_table  <- function(con, tab, colnams, coltypes, keycols, add_keycol = NULL) {
  SQLstr <- paste0("CREATE TABLE `", tab, '` (', paste(paste0('`', colnams, '` ', coltypes),
                                       collapse = ", "), ')')

  res <- RMySQL::dbSendQuery(con, SQLstr)
  DBI::dbClearResult(res)

  if (!is.null(keycols)) {
    if (purrr::is_list(keycols)){
      (1:length(keycols)) %>% purrr::map(function(x) {
        SQLstr <- paste(paste0("CREATE INDEX idx",  x), " on ", paste0("`", tab, "`"),
                        " (", paste(
                          paste0("`", c(keycols[[x]], add_keycol), "`"),
                          collapse = ", "), ")")
        res <- RMySQL::dbSendQuery(con, SQLstr)
        DBI::dbClearResult(res)
      })
    } else {
      if (purrr::is_vector(keycols)){
        SQLstr <- paste0("CREATE INDEX idx on `", tab,
                         "` (", paste(
                           paste0("`", c(keycols, add_keycol), "`"),
                           collapse = ", "), ")")

        res <- RMySQL::dbSendQuery(con, SQLstr)
        DBI::dbClearResult(res)
      }
    }
  }

}

#'Create table based on a dataframe
#'@param con the RMySQL connection
#'@param tab the table name string
#'@param df the dataframe
#'@param keycols the colnums that will form indexes
#'@param unstr determine the structure to be unstructured
#'@export
create_table_df <- function(con, tab, df, keycols, unstr = TRUE) {
  #col_types  <- dplyr::db_data_type(con, df)
  col_types <- RMariaDB::dbDataType(RMariaDB::MariaDB(), obj = df)
  col_names <- names(col_types)

  if (unstr) {
    col_types_mod <- mapply(function(x, y)
      if(!(y %in% keycols) &  grepl("varchar", x)){
        if (grepl('id', tolower(y))) return('varchar(20)') else
          return('varchar(255)')
      } else  {
        if (y == "FieldName") return('varchar(80)') else return('varchar(25)')
      }, x = col_types, y = col_names)
  } else {
    double_str_sz <- function(x, fix_char = NULL) {
      sz <- gsub("[[:lower:]]|[[:punct:]]", "", x) %>% as.numeric() * 4
      sz <- pmin(pmax(sz, 20), 400)
      if (is.null(fix_char)) return(paste0('varchar(', sz, ")")) else return(fix_char)
    }
    col_types_mod <- ifelse(grepl("varchar", col_types),
                            double_str_sz(col_types), col_types)
  }

  create_table(con, tab, colnams = col_names,
               coltypes = col_types_mod, keycols = keycols)
}


#'Insert data onto SQL server
#'@param con the RMySQL connection
#'@param tab the table name string
#'@param df the dataframe
#'@param file_loc the location to write the file to harddrive, when NULL it would be a temporary location on local drive
#'@export
insert_table_df <- function(con, tab, df, file_loc = NULL) {
  tmp_folder <- tempdir()
  if(is.null(file_loc)) {
    tmpfile <- tempfile(pattern = "file", tmpdir = tmp_folder, fileext = ".txt")
  } else {
    tmpfile <- file_loc
  }

  if (!dir.exists(tmp_folder)) dir.create(tmp_folder)

  write.table(df, tmpfile, row.names=FALSE, sep = "|", col.names = F,
              eol = "||")

  SQLstr <-  paste0("LOAD DATA LOCAL INFILE '", gsub("\\\\", "/", tmpfile), "' "
                    , "INTO TABLE ", tab, " "
                    , "FIELDS TERMINATED by '|' "
                    , "ENCLOSED BY '\"' "
                    , "LINES TERMINATED BY '||'")

  resp <- RMySQL::dbSendQuery(con, SQLstr)
  DBI::dbClearResult(resp)
  return(list(resp, SQLstr))
}


#'push a data frame onto SQL server, create new table if doesn't exist
#'@param con the RMySQL connection
#'@param tab the table name string
#'@param df the dataframe
#'@export
push_table_df <- function(con, tbl, df, keycols, ...) {

  if (!RMySQL::dbExistsTable(con, tolower(tbl)) & !RMySQL::dbExistsTable(con, tbl)) create_table_df(con, tab = tbl, df = df,
                                                                 keycols = keycols, ...)
  insert_table_df(con, tab = tbl, df = df)
}


#'push a data frame onto SQL server, create new table if doesn't exist, it also does this by chunks less than 50K to get around limitations from some SQL servers
#'@param con the RMySQL connection
#'@param tab the table name string
#'@param df the dataframe
#'@export
push_table_by_chunck  <- function(con, tbl, df, keycols, ...) {

  n_df <- nrow(df)
  chuncks <- n_df %/% 50e3 + 1

  resp <- list()

  df <- df %>%
    dplyr::mutate(chunck = base::sample(seq_len(chuncks), size = n_df, replace = TRUE))

  for (i in seq_len(chuncks)) {
    resp[[i]] <- push_table_df(con = con, tbl = tbl,
                                            df = df %>%
                                              dplyr::filter(chunck == i) %>%
                                              dplyr::select(-chunck),
                                            keycols = keycols, ...
    )
  }
  return(resp)
}

#'show indexes of a table
#'@param con the RMySQL connection
#'@param tab the table name string
#'@export
show_indexes  <- function(con, tab) {
  sql <- paste0('SHOW INDEX FROM ', tab)
  res <- DBI::dbSendQuery(con, sql)
  resp <- DBI::dbFetch(res)
  dbClearResult(res)
  return(resp)
}



#delete records based on another table
#'@param con the RMySQL connection
#'@param tab the table name string
#'@param df the dataframe to delete
#'@param keycols key columns
#'@export
delete_by_df  <- function(con, tab, df, keycols) {
  temp_mapping <- paste0('temp_mapping',
                         sample(letters, 10) %>% paste(collapse = ""))

  RMySQL::dbWriteTable(con, temp_mapping,
                         df %>% select_(.dots = keycols)
                         , row.names = F)
  sql_str <- paste0('delete `' , tab, '` from `', tab, '` INNER JOIN ', temp_mapping,
                    ' ON ', paste0('`', tab, "`", ".", keycols, ' = `',
                           temp_mapping, '`.', keycols ) %>%
                      paste(collapse = " and ")
  )
  cat(sql_str, ' being run /n')
  res <- DBI::dbSendQuery(con,  sql_str)
  DBI::dbClearResult(res)

  droptables(con, temp_mapping)
  return(res)
}
