pybind11_add_module(
        pybind11_abseil_status_module
        status.cc
)

target_link_libraries(
        pybind11_abseil_status_module PRIVATE
        pybind11::module
        register_status_bindings
        pybind11_abseil::status_casters
        pybind11_abseil::absl_casters
        pybind11_abseil::import_status_module
        pybind11_abseil::statusor_caster
)