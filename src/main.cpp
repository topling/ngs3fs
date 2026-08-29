#include "io.hpp"

#include <iostream>
#include <string_view>

int run(int argc, char** argv);

int main(int argc, char** argv) {
  if (argc == 2 && std::string_view(argv[1]) == "--self-test") {
    const auto pipe = Pipe::create();
    std::cout << "ngs3fs self-test: preferred_io="
              << kPreferredIoSize
              << " pipe_capacity=" << pipe.capacity() << '\n';
    return 0;
  }

  return run(argc, argv);
}
