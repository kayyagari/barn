use sqlparser::parser::{Parser, ParserError};
use sqlparser::dialect::GenericDialect;

#[cfg(test)]
mod tests {
    use sqlparser::parser::Parser;
    use sqlparser::dialect::GenericDialect;

    #[test]
    fn test_parse() {
        let dialect = GenericDialect{};
        let ast = Parser::parse_sql(&dialect, "SELECT @b.\"a[0].b\", @b.id, (c * 2) as x FROM Business b").unwrap();
        println!("{:?}", ast);
    }
}
