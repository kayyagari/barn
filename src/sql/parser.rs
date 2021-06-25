use sqlparser::parser::{Parser, ParserError};
use sqlparser::dialect::GenericDialect;
use sqlparser::ast::Statement::Query;
use sqlparser::ast::{Select, SetExpr, SelectItem, Expr};
use crate::sql::expressions::Column;

pub fn parse(query: String) {
    let dialect = GenericDialect{};
    let ast = Parser::parse_sql(&dialect, query.as_str()).unwrap();
    for stmt in &ast {
        if let Query(stmt) = stmt {
            if let SetExpr::Select(select) = stmt.body {
                for p in &select.projection {
                    match p {
                        SelectItem::Wildcard => {
                            let c = Column{
                                name: String::from("*")
                            };
                        },
                        _ => {

                        }
                    }
                }
                if let Some(expr) = select.selection {
                    if let Expr::BinaryOp(l, o, p) =  expr {

                    }
                }
            }
        }
    }
}

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
