fn main() {
    capnpc::CompilerCommand::new()
        .file("buffer.capnp")
        .run()
        .expect("compiling buffer.capnp");
}
