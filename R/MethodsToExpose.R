# Concept Set Diagnostics -------------------------

##  Helpers -------------------------------------
checkIfConceptSetInstantiated <- function(connection,
                                          tempEmulationSchema,
                                          databaseSchema = NULL) {
  
  tbls <- DatabaseConnector::getTableNames(connection, databaseSchema = databaseSchema)
  "inst_concept_sets" %in% tbls
}

extractConceptSetsSqlFromCohortSql <- function(cohortSql) {
  if (length(cohortSql) > 1) {
    stop("Please check if more than one cohort SQL was provided.")
  }
  sql <- gsub("with primary_events.*", "", cohortSql)
  
  # Find opening and closing parentheses:
  starts <- stringr::str_locate_all(sql, "\\(")[[1]][, 1]
  ends <- stringr::str_locate_all(sql, "\\)")[[1]][, 1]
  
  x <- rep(0, nchar(sql))
  x[starts] <- 1
  x[ends] <- -1
  level <- cumsum(x)
  level0 <- which(level == 0)
  
  subQueryLocations <-
    stringr::str_locate_all(sql, "SELECT [0-9]+ as codeset_id")[[1]]
  subQueryCount <- nrow(subQueryLocations)
  conceptsetSqls <- vector("character", subQueryCount)
  conceptSetIds <- vector("integer", subQueryCount)
  
  temp <- list()
  if (subQueryCount > 0) {
    for (i in 1:subQueryCount) {
      startForSubQuery <- min(starts[starts > subQueryLocations[i, 2]])
      endForSubQuery <- min(level0[level0 > startForSubQuery])
      subQuery <-
        paste(
          stringr::str_sub(sql, subQueryLocations[i, 1], endForSubQuery),
          "C"
        )
      conceptsetSqls[i] <- subQuery
      conceptSetIds[i] <- stringr::str_replace(
        subQuery,
        pattern = stringr::regex(
          pattern = "SELECT ([0-9]+) as codeset_id.*",
          ignore_case = TRUE,
          multiline = TRUE,
          dotall = TRUE
        ),
        replacement = "\\1"
      ) %>%
        utils::type.convert(as.is = TRUE)
      temp[[i]] <- tibble::tibble(
        conceptSetId = conceptSetIds[i],
        conceptSetSql = conceptsetSqls[i]
      )
    }
  } else {
    temp <- tibble::tibble()
  }
  return(dplyr::bind_rows(temp))
}


extractConceptSetsJsonFromCohortJson <- function(cohortJson) {
  cohortDefinition <-
    RJSONIO::fromJSON(content = cohortJson, digits = 23)
  if ("expression" %in% names(cohortDefinition)) {
    expression <- cohortDefinition$expression
  } else {
    expression <- cohortDefinition
  }
  conceptSetExpression <- list()
  if (length(expression$ConceptSets) > 0) {
    for (i in (1:length(expression$ConceptSets))) {
      conceptSetExpression[[i]] <-
        tibble::tibble(
          conceptSetId = expression$ConceptSets[[i]]$id,
          conceptSetName = expression$ConceptSets[[i]]$name,
          conceptSetExpression = expression$ConceptSets[[i]]$expression$items %>% RJSONIO::toJSON(digits = 23)
        )
    }
  } else {
    conceptSetExpression <- tibble::tibble()
  }
  return(dplyr::bind_rows(conceptSetExpression))
}

combineConceptSetsFromCohorts <- function(cohorts) {
  # cohorts should be a dataframe with at least cohortId, sql and json
  
  errorMessage <- checkmate::makeAssertCollection()
  checkmate::assertDataFrame(
    x = cohorts,
    min.cols = 4,
    add = errorMessage
  )
  checkmate::assertNames(
    x = colnames(cohorts),
    must.include = c("cohortId", "sql", "json", "cohortName")
  )
  checkmate::reportAssertions(errorMessage)
  checkmate::assertDataFrame(
    x = cohorts %>% dplyr::select(
      cohortId,
      sql,
      json,
      cohortName
    ),
    any.missing = FALSE,
    min.cols = 4,
    add = errorMessage
  )
  checkmate::reportAssertions(errorMessage)
  
  conceptSets <- list()
  conceptSetCounter <- 0
  
  for (i in (1:nrow(cohorts))) {
    cohort <- cohorts[i, ]
    sql <-
      extractConceptSetsSqlFromCohortSql(cohortSql = cohort$sql)
    json <-
      extractConceptSetsJsonFromCohortJson(cohortJson = cohort$json)
    
    if (nrow(sql) == 0 || nrow(json) == 0) {
      ParallelLogger::logInfo(
        "Cohort Definition expression does not have a concept set expression. ",
        "Skipping Cohort: ",
        cohort$cohortName
      )
    } else {
      if (!length(sql$conceptSetId %>% unique()) == length(json$conceptSetId %>% unique())) {
        stop(
          "Mismatch in concept set IDs between SQL and JSON for cohort ",
          cohort$cohortFullName
        )
      }
      if (length(sql) > 0 && length(json) > 0) {
        conceptSetCounter <- conceptSetCounter + 1
        conceptSets[[conceptSetCounter]] <-
          tibble::tibble(
            cohortId = cohort$cohortId,
            dplyr::inner_join(x = sql, y = json, by = "conceptSetId")
          )
      }
    }
  }
  if (length(conceptSets) == 0) {
    return(NULL)
  }
  conceptSets <- dplyr::bind_rows(conceptSets) %>%
    dplyr::arrange(cohortId, conceptSetId)
  
  uniqueConceptSets <- conceptSets %>%
    dplyr::select(conceptSetExpression) %>%
    dplyr::distinct() %>%
    dplyr::mutate(uniqueConceptSetId = dplyr::row_number())
  
  conceptSets <- conceptSets %>%
    dplyr::inner_join(uniqueConceptSets, by = "conceptSetExpression") %>%
    dplyr::distinct() %>%
    dplyr::relocate(
      uniqueConceptSetId,
      cohortId,
      conceptSetId
    ) %>%
    dplyr::arrange(
      uniqueConceptSetId,
      cohortId,
      conceptSetId
    )
  return(conceptSets)
}


mergeTempTables <-
  function(connection,
           tableName,
           tempTables,
           tempEmulationSchema) {
    valueString <-
      paste(tempTables, collapse = "\n\n  UNION ALL\n\n  SELECT *\n  FROM ")
    sql <-
      sprintf(
        "SELECT *\nINTO %s\nFROM (\n  SELECT *\n  FROM %s\n) tmp;",
        tableName,
        valueString
      )
    sql <-
      SqlRender::translate(sql,
                           targetDialect = connection@dbms,
                           tempEmulationSchema = tempEmulationSchema
      )
    DatabaseConnector::executeSql(connection,
                                  sql,
                                  progressBar = FALSE,
                                  reportOverallTime = FALSE
    )
    
    # Drop temp tables:
    for (tempTable in tempTables) {
      sql <-
        sprintf("TRUNCATE TABLE %s;\nDROP TABLE %s;", tempTable, tempTable)
      sql <-
        SqlRender::translate(sql,
                             targetDialect = connection@dbms,
                             tempEmulationSchema = tempEmulationSchema
        )
      DatabaseConnector::executeSql(connection,
                                    sql,
                                    progressBar = FALSE,
                                    reportOverallTime = FALSE
      )
    }
  }

instantiateUniqueConceptSets <- function(uniqueConceptSets,
                                         connection,
                                         cdmDatabaseSchema,
                                         vocabularyDatabaseSchema = cdmDatabaseSchema,
                                         tempEmulationSchema,
                                         conceptSetsTable = "#inst_concept_sets") {
  ParallelLogger::logInfo("Instantiating concept sets")
  sql <- sapply(
    split(uniqueConceptSets, 1:nrow(uniqueConceptSets)),
    function(x) {
      sub(
        "SELECT [0-9]+ as codeset_id",
        sprintf("SELECT %s as codeset_id", x$uniqueConceptSetId),
        x$conceptSetSql
      )
    }
  )
  
  batchSize <- 100
  tempTables <- c()
  pb <- utils::txtProgressBar(style = 3)
  for (start in seq(1, length(sql), by = batchSize)) {
    utils::setTxtProgressBar(pb, start / length(sql))
    tempTable <-
      paste("#", paste(sample(letters, 20, replace = TRUE), collapse = ""), sep = "")
    tempTables <- c(tempTables, tempTable)
    end <- min(start + batchSize - 1, length(sql))
    sqlSubset <- sql[start:end]
    sqlSubset <- paste(sqlSubset, collapse = "\n\n  UNION ALL\n\n")
    sqlSubset <-
      sprintf(
        "SELECT *\nINTO %s\nFROM (\n %s\n) tmp;",
        tempTable,
        sqlSubset
      )
    sqlSubset <-
      SqlRender::render(sqlSubset, vocabulary_database_schema = vocabularyDatabaseSchema)
    sqlSubset <- SqlRender::translate(sqlSubset,
                                      targetDialect = connection@dbms,
                                      tempEmulationSchema = tempEmulationSchema
    )
    DatabaseConnector::executeSql(connection,
                                  sqlSubset,
                                  progressBar = FALSE,
                                  reportOverallTime = FALSE
    )
  }
  utils::setTxtProgressBar(pb, 1)
  close(pb)
  
  mergeTempTables(
    connection = connection,
    tableName = conceptSetsTable,
    tempTables = tempTables,
    tempEmulationSchema = tempEmulationSchema
  )
}

getCodeSetId <- function(criterion) {
  if (is.list(criterion)) {
    criterion$CodesetId
  } else if (is.vector(criterion)) {
    return(criterion["CodesetId"])
  } else {
    return(NULL)
  }
}

getCodeSetIds <- function(criterionList) {
  codeSetIds <- lapply(criterionList, getCodeSetId)
  codeSetIds <- do.call(c, codeSetIds)
  if (is.null(codeSetIds)) {
    return(NULL)
  } else {
    return(dplyr::tibble(domain = names(criterionList), codeSetIds = codeSetIds)
           %>% filter(!is.na(codeSetIds)))
  }
}

pasteIds <- function(row) {
  
  res <- tibble::tibble(
    domain = row$domain[1],
    uniqueConceptSetId = paste(row$uniqueConceptSetId, collapse = ", ")
  )
  
  return(res)
}



cohortEntryBreakdown <- function(connection,
                                 cdmDatabaseSchema,
                                 vocabularyDatabaseSchema,
                                 cohortDatabaseSchema, 
                                 cohortTable,
                                 tempEmulationSchema,
                                 cohortId, 
                                 domain, 
                                 uniqueConceptSetId) {
  
  dd <- fs::path_package("CohortDiagnostics", "csv/domains.csv") %>%
    readr::read_csv(show_col_types = FALSE) %>%
    dplyr::filter(domain == !!domain)
  
  sql <- fs::path_package("CohortDiagnostics", "sql/sql_server/CohortEntryBreakdown.sql") %>%
    readr::read_file() %>%
    SqlRender::render(
      cdm_database_schema = cdmDatabaseSchema,
      vocabulary_database_schema = vocabularyDatabaseSchema,
      cohort_database_schema = cohortDatabaseSchema,
      cohort_table = cohortTable,
      cohort_id = cohortId,
      domain_table = dd$domainTable,
      domain_start_date = dd$domainStartDate,
      domain_concept_id = dd$domainConceptId,
      domain_source_concept_id = dd$domainSourceConceptId,
      use_source_concept_id = !(is.na(dd$domainSourceConceptId) | is.null(dd$domainSourceConceptId)),
      primary_codeset_ids = uniqueConceptSetId,
      concept_set_table = "#inst_concept_sets",
      store = TRUE,
      store_table = "#breakdown"
    ) %>%
    SqlRender::translate(targetDialect = connection@dbms,
                         tempEmulationSchema = tempEmulationSchema)
  
  DatabaseConnector::executeSql(
    connection = connection,
    sql = sql,
    progressBar = FALSE,
    reportOverallTime = FALSE
  )
  
  
  counts <-
    DatabaseConnector::renderTranslateQuerySql(
      connection = connection,
      sql = "SELECT * FROM @store_table;",
      tempEmulationSchema = tempEmulationSchema,
      store_table = "#breakdown",
      snakeCaseToCamelCase = TRUE
    ) %>%
    tibble::tibble()
  
  #Clean up
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = "TRUNCATE TABLE @store_table;\nDROP TABLE @store_table;",
    tempEmulationSchema = tempEmulationSchema,
    store_table = "#breakdown",
    progressBar = FALSE,
    reportOverallTime = FALSE
  )
  
  
  return(counts)
}


## Methods --------------------------------


runConceptSetCounts <- function(connection,
                                cdmDatabaseSchema,
                                writeSchema) {
  
  
  check <- checkIfConceptSetInstantiated(connection = connection,
                                         tempEmulationSchema = tempEmulationSchema)
  
  if (!check) {
    #Instantuiate Unique Concepts
    instantiateUniqueConceptSets(
      uniqueConceptSets = uniqueConceptSets,
      connection = connection,
      cdmDatabaseSchema = cdmDatabaseSchema,
      vocabularyDatabaseSchema = vocabularyDatabaseSchema,
      tempEmulationSchema = tempEmulationSchema,
      conceptSetsTable = "#inst_concept_sets"
    )
  }
  
  ParallelLogger::logInfo("Creating internal concept counts table")
  sql <- fs::path_package("CohortDiagnostics", "sql/sql_server/CreateConceptCountTable.sql") %>%
    readr::read_file() %>%
    SqlRender::render(cdm_database_schema = cdmDatabaseSchema,
                      work_database_schema = writeSchema, 
                      concept_counts_table = "concept_counts",
                      table_is_temp = FALSE) %>%
    SqlRender::translate(targetDialect = connection@dbms,
                         tempEmulationSchema = tempEmulationSchema)
  DatabaseConnector::executeSql(connection, sql)
  
  #collect counts of source codes
  counts <- DatabaseConnector::renderTranslateQuerySql(
    connection = connection,
    sql = "SELECT * FROM @concept_counts_table;",
    concept_counts_table = "#concept_counts",
    tempEmulationSchema = tempEmulationSchema,
    snakeCaseToCamelCase = TRUE
  )
  
  return(counts)

}


runIncludedSourceConcepts <- function(connection,
                                      cdmDatabaseSchema,
                                      tempEmulationSchema,
                                      databaseId) {
  
  check <- checkIfConceptSetInstantiated(connection = connection,
                                tempEmulationSchema = tempEmulationSchema)
  
  if (!check) {
    #Instantuiate Unique Concepts
    instantiateUniqueConceptSets(
      uniqueConceptSets = uniqueConceptSets,
      connection = connection,
      cdmDatabaseSchema = cdmDatabaseSchema,
      vocabularyDatabaseSchema = vocabularyDatabaseSchema,
      tempEmulationSchema = tempEmulationSchema,
      conceptSetsTable = "#inst_concept_sets"
    )
  }
  
  #Run cohort source codes sql
  sql <- fs::path_package("CohortDiagnostics", "sql/sql_server/CohortSourceCodes.sql") %>%
    readr::read_file() %>%
    SqlRender::render(cdm_database_schema = cdmDatabaseSchema,
                      instantiated_concept_sets = "#inst_concept_sets",
                      include_source_concept_table = "#inc_src_concepts",
                      by_month = FALSE) %>%
    SqlRender::translate(targetDialect = connection@dbms,
                         tempEmulationSchema = tempEmulationSchema)
  DatabaseConnector::executeSql(connection, sql)
  
  #collect counts of source codes
  counts <- DatabaseConnector::renderTranslateQuerySql(
    connection = connection,
    sql = "SELECT * FROM @include_source_concept_table;",
    include_source_concept_table = "#inc_src_concepts",
    tempEmulationSchema = tempEmulationSchema,
    snakeCaseToCamelCase = TRUE
  ) %>%
    tibble::tibble() %>%
    dplyr::rename(uniqueConceptSetId = conceptSetId) %>%
    dplyr::inner_join(
      conceptSets %>% dplyr::select(
        uniqueConceptSetId,
        cohortId,
        conceptSetId
      ),
      by = "uniqueConceptSetId", relationship = "many-to-many"
    ) %>%
    dplyr::select(-uniqueConceptSetId) %>%
    dplyr::mutate(databaseId = !!databaseId) %>%
    dplyr::relocate(
      databaseId,
      cohortId,
      conceptSetId,
      conceptId
    ) %>%
    dplyr::distinct() %>%
    dplyr::group_by(
      databaseId,
      cohortId,
      conceptSetId,
      conceptId,
      sourceConceptId
    ) %>%
    dplyr::summarise(
      conceptCount = max(conceptCount),
      conceptSubjects = max(conceptSubjects)
    ) %>%
    dplyr::ungroup()
  
  return(counts)
}


runBreakdownIndexEvents <- function(connection,
                                    cdmDatabaseSchema,
                                    vocabularyDatabaseSchema,
                                    cohortDatabaseSchema, 
                                    cohortTable,
                                    tempEmulationSchema,
                                    cohort) {
  
  
  
  
  cohortId <- cohort$cohortId
  
  ParallelLogger::logInfo(
    "- Breaking down index events for cohort '",
    cohort$cohortName,
    "'"
  )
  
  domains <- fs::path_package("CohortDiagnostics", "csv/domains.csv") %>%
    readr::read_csv(show_col_types = FALSE)
  
  cohortDefinition <-
    RJSONIO::fromJSON(cohort$json, digits = 23)
  primaryCodesetIds <- purrr::map_dfr(
    cohortDefinition$PrimaryCriteria$CriteriaList, ~getCodeSetIds(.x)
  )
  
  if (nrow(primaryCodesetIds) == 0) {
    warning(
      "No primary event criteria concept sets found for cohort id: ",
      cohortId
    )
    return(tibble:tibble())
  }
  primaryCodesetIds <- primaryCodesetIds %>% dplyr::filter(domain %in%
                                                             c(domains$domain %>% unique()))
  if (nrow(primaryCodesetIds) == 0) {
    warning(
      "Primary event criteria concept sets found for cohort id: ",
      cohortId, " but,", "\nnone of the concept sets belong to the supported domains.",
      "\nThe supported domains are:\n", paste(domains$domain,
                                              collapse = ", "
      )
    )
    return(tibble::tibble())
  }
  primaryCodesetIds <- conceptSets %>%
    dplyr::filter(cohortId %in% cohortId) %>%
    dplyr::select(codeSetIds = conceptSetId, uniqueConceptSetId) %>%
    dplyr::inner_join(primaryCodesetIds, by = "codeSetIds") 
  
  primaryCodesetIds <- split(primaryCodesetIds, primaryCodesetIds$domain) %>%
    purrr::map_dfr(~pasteIds(.x))
  
  counts <- 
    purrr::pmap_dfr(primaryCodesetIds, ~cohortEntryBreakdown(
      connection = connection,
      cdmDatabaseSchema = cdmDatabaseSchema,
      vocabularyDatabaseSchema = cdmDatabaseSchema,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = cohortTable,
      tempEmulationSchema = tempEmulationSchema,
      cohortId = cohortId, 
      domain = ..1, 
      uniqueConceptSetId = ..2))
  
  return(counts)
  
}


runOrphanConcepts <- function(connection,
                              cdmDatabaseSchema,
                              vocabularyDatabaseSchema,
                              tempEmulationSchema,
                              useCodesetTable = TRUE,
                              codesetId,
                              conceptIds = c(),
                              conceptCountsDatabaseSchema,
                              conceptCountsTable = "concept_counts",
                              conceptCountsTableIsTemp = FALSE,
                              instantiatedCodeSets = "#inst_concept_sets",
                              orphanConceptTable = "#orphan_concepts") {
  
  
  
  
  sql <- fs::path_package("CohortDiagnostics", "sql/sql_server/OrphanCodes.sql") %>%
    readr::read_file() %>%
    SqlRender::render(
      vocabulary_database_schema = vocabularyDatabaseSchema,
      work_database_schema = conceptCountsDatabaseSchema,
      concept_counts_table = conceptCountsTable,
      concept_counts_table_is_temp = conceptCountsTableIsTemp,
      concept_ids = conceptIds,
      use_codesets_table = useCodesetTable,
      orphan_concept_table = orphanConceptTable,
      instantiated_code_sets = instantiatedCodeSets,
      codeset_id = codesetId
    ) %>%
    SqlRender::translate(
      targetDialect = connection@dbms,
      tempEmulationSchema = tempEmulationSchema)
  
  
  DatabaseConnector::executeSql(connection, sql)
  ParallelLogger::logTrace("- Fetching orphan concepts from server")
  sql <- "SELECT * FROM @orphan_concept_table;"
  orphanConcepts <-
    DatabaseConnector::renderTranslateQuerySql(
      sql = sql,
      connection = connection,
      tempEmulationSchema = tempEmulationSchema,
      orphan_concept_table = orphanConceptTable,
      snakeCaseToCamelCase = TRUE
    ) %>%
    tibble::tibble()
  
  #clean up
  ParallelLogger::logTrace("- Dropping orphan temp tables")
  sql <- fs::path_package("CohortDiagnostics", "sql/sql_server/DropOrphanConceptTempTables.sql") %>%
    readr::read_file() %>%
    SqlRender::render() %>%
    SqlRender::translate(
      targetDialect = connection@dbms,
      tempEmulationSchema = tempEmulationSchema
    )
  DatabaseConnector::executeSql(
    connection = connection,
    sql = sql,
    progressBar = FALSE,
    reportOverallTime = FALSE
  )
  
  return(orphanConcepts)
  
}

runVisitContext <- function(connection,
                            cdmDatabaseSchema,
                            cohortDatabaseSchema,
                            tempEmulationSchema,
                            cohortTable,
                            cohortIds) {
  
  sql <- fs::path_package("CohortDiagnostics", "sql/sql_server/VisitContext.sql") %>%
    readr::read_file() %>%
    SqlRender::render(
      visit_context_table = "#visit_context",
      cdm_database_schema = cdmDatabaseSchema,
      cohort_database_schema = cohortDatabaseSchema,
      cohort_table = cohortTable,
      cohort_ids = cohortIds
    )
  DatabaseConnector::executeSql(connection, sql)
  
  ParallelLogger::logTrace("- Fetching visit context from server")
  sql <- "SELECT * FROM @orphan_concept_table;"
  visitContext <-
    DatabaseConnector::renderTranslateQuerySql(
      sql = sql,
      connection = connection,
      tempEmulationSchema = tempEmulationSchema,
      visit_context_table = "#visit_context",
      snakeCaseToCamelCase = TRUE
    ) %>%
    tibble::tibble()
  
  return(visitContext)
}


runObservationDateRange <- function(connection,
                                    cdmDatabaseSchema,
                                    tempEmulationSchema) {
  
  observationPeriodDateRange <- renderTranslateQuerySql(
    connection = connection,
    sql = "SELECT MIN(observation_period_start_date) observation_period_min_date,
             MAX(observation_period_end_date) observation_period_max_date,
             COUNT(distinct person_id) persons,
             COUNT(person_id) records,
             SUM(CAST(DATEDIFF(dd, observation_period_start_date, observation_period_end_date) AS BIGINT)) person_days
             FROM @cdm_database_schema.observation_period;",
    cdm_database_schema = cdmDatabaseSchema,
    snakeCaseToCamelCase = TRUE,
    tempEmulationSchema = tempEmulationSchema
  )
  
  return(observationPeriodDateRange)
  
}

