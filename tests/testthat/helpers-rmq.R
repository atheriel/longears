amqp_declare_tmp_exchange <- function(conn, type = "direct") {
  name <- random_name()
  amqp_declare_exchange(conn, name, type = type, auto_delete = TRUE)
  name
}

pool <- c(LETTERS, letters, 0:9)
random_name <- function() {
  rand <- sample(pool, 16, replace = TRUE)
  sprintf("longears-%s", paste(rand, collapse = ""))
}
