// For more information on writing tests, see
// https://scalameta.org/munit/docs/getting-started.html

import scala.util.boundary, boundary.break

class MySuite extends munit.FunSuite {
  test("example test that succeeds") {
    val obtained = 42
    val expected = 42
    assertEquals(obtained, expected)

    boundary{
        for i <- 0 to 10 do 
            if i == 5 then
                break()
            println(i)
    }
  }
}
