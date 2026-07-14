pub use terms::*;

mod terms {
    use crate::ast::TokenView;

    #[derive(Debug, PartialEq)]
    pub enum BindMarker {
        Anonymous,
        Named(TokenView),
    }
}
