#ifndef THIRD_PARTY_EGLT_PYBIND11_TYPES_H_
#define THIRD_PARTY_EGLT_PYBIND11_TYPES_H_

#include <string_view>

#include <pybind11/pybind11.h>

namespace py = ::pybind11;

namespace eglt {



namespace pybindings {

void BindChunkMetadata(py::handle scope,
                           std::string_view name = "ChunkMetadata");

void BindChunk(py::handle scope, std::string_view name = "Chunk");

void BindNodeFragment(py::handle scope,
                          std::string_view name = "NodeFragment");

void BindNamedParameter(py::handle scope,
                            std::string_view name = "NamedParameter");

void BindActionMessage(py::handle scope,
                           std::string_view name = "ActionMessage");

void BindSessionMessage(py::handle scope,
                            std::string_view name = "SessionMessage");

py::module_ MakeTypesModule(py::module_ scope,
                            std::string_view module_name = "types");
}  // namespace pybindings

}  // namespace eglt

#endif  // THIRD_PARTY_EGLT_PYBIND11_TYPES_H_
