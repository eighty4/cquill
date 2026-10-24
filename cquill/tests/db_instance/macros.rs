macro_rules! vec_of_strings {
    ($($x:expr),* $(,)?) => {
        vec![$(String::from($x)),*]
    };
}

pub(crate) use vec_of_strings;
