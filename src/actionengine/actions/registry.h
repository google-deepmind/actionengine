// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef ACTIONENGINE_ACTIONS_REGISTRY_H_
#define ACTIONENGINE_ACTIONS_REGISTRY_H_

#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include <absl/container/flat_hash_map.h>

#include "actionengine/actions/schema.h"
#include "actionengine/data/types.h"

/** @file
 *  @brief
 *    A registry for ActionEngine actions.
 *
 * This file contains the definition of the ActionRegistry class, which is used
 * to register actions with their schemas and handlers, and provides methods to
 * create action messages and instances based on the registered schemas.
 */

namespace act {

/** A registry for ActionEngine actions.
 *
 * This class is used to register actions with their schemas and handlers, and
 * provides methods to create action messages and instances based on the
 * registered schemas.
 *
 * @headerfile actionengine/actions/registry.h
 */
class ActionRegistry {
 public:
  /** @brief
   *    Registers an action with the given name, schema, and handler.
   *
   * This method adds the action to the registry, allowing it to be created
   * later using MakeAction or MakeActionMessage.
   *
   * @param name
   *   The name of the action. Must be unique within the registry.
   * @param schema
   *   The schema of the action, defining its inputs and outputs.
   * @param handler
   *   The handler function that will be called when the action is executed.
   */
  void Register(std::string_view name, const ActionSchema& schema,
                const ActionHandler& handler);

  /** @brief
   *    Checks if an action with the given name is registered.
   *
   * Notice that when dealing with a particular registry whose content you
   * are not sure about, you MUST always check if the action is registered
   * before using any other methods, as otherwise they will fail, terminating
   * the program with a CHECK failure.
   *
   * @param name
   *   The name of the action to check.
   * @return
   *   True if the action is registered, false otherwise.
   */
  [[nodiscard]] bool IsRegistered(std::string_view name) const;

  /** @brief
   *    Creates an ActionMessage for the action with the given name and ID.
   *
   * This method generates an ActionMessage that can be used to call the action
   * by sending it in a SessionMessage over a WireStream.
   *
   * Note: TERMINATES if the action is not registered, so you must use
   * IsRegistered() before calling this method to ensure the action exists.
   *
   * @param name
   *   The name of the action.
   * @param id
   *   The ID of the action instance. Must not be empty.
   * @return
   *   An ActionMessage representing the action with the given name and ID.
   */
  [[nodiscard]] ActionMessage MakeActionMessage(std::string_view name,
                                                std::string_view id) const;

  /** @brief
   *    Creates an action instance based on the registered schema and handler.
   *
   * This method creates an Action object that can be used to call or run the
   * action with the specified inputs and outputs. The action will be created
   * with the schema and handler registered under the given name.
   *
   * Note: TERMINATES if the action is not registered, so you must use
   * IsRegistered() before calling this method to ensure the action exists.
   *
   * @param name
   *   The key of the action to create, which must be registered.
   * @param action_id
   *   The ID of the action instance. If empty, a unique ID will be generated.
   * @param inputs
   *   A vector of input ports for the action.
   * @param outputs
   *   A vector of output ports for the action.
   * @return
   *   An owning pointer to the newly created Action instance.
   */
  [[nodiscard]] std::unique_ptr<Action> MakeAction(
      std::string_view name, std::string_view action_id = "",
      std::vector<Port> inputs = {}, std::vector<Port> outputs = {}) const;

  /** @brief
   *    Gets the schema of the action with the given name.
   *
   * Note: TERMINATES if the action is not registered, so you must use
   * IsRegistered() before calling this method to ensure the action exists.
   *
   * @param name
   *   The name of the action whose schema is requested.
   * @return
   *   The ActionSchema of the action with the given name.
   */
  [[nodiscard]] const ActionSchema& GetSchema(std::string_view name) const;

  /** @brief
   *    Gets the handler of the action with the given name.
   *
   * Note: TERMINATES if the action is not registered, so you must use
   * IsRegistered() before calling this method to ensure the action exists.
   *
   * @param name
   *   The name of the action whose handler is requested.
   * @return
   *   The ActionHandler of the action with the given name.
   */
  [[nodiscard]] const ActionHandler& GetHandler(std::string_view name) const;

  absl::flat_hash_map<std::string, ActionSchema> schemas_;
  absl::flat_hash_map<std::string, ActionHandler> handlers_;
};
}  // namespace act

#endif  // ACTIONENGINE_ACTIONS_REGISTRY_H_