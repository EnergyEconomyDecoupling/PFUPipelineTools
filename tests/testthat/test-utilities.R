test_that("self_name() works as intended", {
  c("a", "b", "c") |>
    self_name() |>
    expect_equal(c(a = "a", b = "b", c = "c"))

  list("A", "B", "C") |>
    self_name() |>
    expect_equal(list(A = "A", B = "B", C = "C"))
})
