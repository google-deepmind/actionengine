#ifndef EGLT_PYBIND11_EGLT_NODES_H_
#define EGLT_PYBIND11_EGLT_NODES_H_

#include <string_view>

#include <pybind11/pybind11.h>

namespace eglt::pybindings {

namespace py = ::pybind11;

void BindNodeMap(py::handle scope, std::string_view name = "NodeMap");

void BindAsyncNode(py::handle scope, std::string_view name = "AsyncNode");

}  // namespace eglt::pybindings

#endif  // EGLT_PYBIND11_EGLT_NODES_H_
