
/// A logical plan representing the query to be executed
pub trait LogicalPlan {
    fn children(&self) -> Vec<LogicalPlan>;
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum FieldDataType {
    String,
    Integer,
    Float,
    Boolean,
    Array,
    Any
}

pub struct Field {
    pub name: String,
    pub data_type: FieldType
}

pub trait LogicalExpr {
    fn to_field(&self, p: &LogicalPlan) -> Field;
}

pub struct Column {
    pub name: String,
}

impl LogicalExpr for Column {
    fn to_field(&self, lp: &impl LogicalPlan) -> Field {
        Field{
            name: self.name.clone(),
            data_type: FieldDataType::Any
        }
    }
}

pub struct LiteralString {
    pub given: String,
}

impl LogicalExpr for LiteralString {
    fn to_field(&self, lp: &impl LogicalPlan) -> Field {
        Field{
            name: self.name.clone(),
            data_type: FieldDataType::String
        }
    }
}

pub struct LiteralInteger {
    pub given: i64,
}

impl LogicalExpr for LiteralInteger {
    fn to_field(&self, lp: &impl LogicalPlan) -> Field {
        Field{
            name: self.name.clone(),
            data_type: FieldDataType::Integer
        }
    }
}

pub struct LiteralFloat {
    pub given: f64,
}

impl LogicalExpr for LiteralFloat {
    fn to_field(&self, lp: &impl LogicalPlan) -> Field {
        Field{
            name: self.name.clone(),
            data_type: FieldDataType::Float
        }
    }
}

pub enum BooleanBinaryExpr {
    Eq(LogicalExpr, LogicalExpr)
}

impl LogicalExpr for BooleanBinaryExpr {
    fn to_field(&self, lp: &impl LogicalPlan) -> Field {
        match self {
            Eq(expr) => {
                Field{
                    name: String::from("eq"),
                    data_type: FieldDataType::Boolean
                }
            },
            _ => {
                panic!("unsupported expression");
            }
        }
    }
}