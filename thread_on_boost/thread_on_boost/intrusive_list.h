// Copyright 2011 Google Inc. All Rights Reserved.
// Author: chandlerc@google.com (Chandler Carruth)
// Based on the interface written by pmattis@google.com (Peter Mattis).
//
// An intrusive_list<> is a doubly-linked list where the link pointers are
// embedded in the elements. They are circularly linked making insertion and
// removal into a known position constant time and branch-free operations.
//
// Usage is similar to an STL list<> where feasible, but there are important
// differences. First and foremost, the elements must derive from the
// intrusive_link<> base class:
//
//   struct Foo : public gtl::intrusive_link<Foo> {
//     // ...
//   }
//
//   intrusive_list<Foo> l;
//   l.push_back(new Foo);
//   l.push_front(new Foo);
//   l.erase(&l.front());
//   l.erase(&l.back());
//
// Intrusive lists are primarily useful when you would have considered embedding
// link pointers in your class directly for space or performance reasons. An
// intrusive_link<> is the size of 2 pointers, usually 16 bytes on 64-bit
// systems. Intrusive lists do not perform memory allocation (unlike the STL
// list<> class) and thus may use less memory than list<>. In particular, if the
// list elements are pointers to objects, using a list<> would perform an extra
// memory allocation for each list node structure, while an intrusive_list<>
// would not.
//
// Note that intrusive_link is exempt from the C++ style guide's limitations on
// multiple inheritance, so it's fine to inherit from both intrusive_link and a
// base class, even if the base class is not a pure interface.
//
// Because the list pointers are embedded in the objects stored in an
// intrusive_list<>, erasing an item from a list is constant time. Consider the
// following:
//
//   map<string,Foo> foo_map;
//   list<Foo*> foo_list;
//
//   foo_list.push_back(&foo_map["bar"]);
//   foo_list.erase(&foo_map["bar"]); // Compile error!
//
// The problem here is that a Foo* doesn't know where on foo_list it resides,
// so removal requires iteration over the list. Various tricks can be performed
// to overcome this. For example, a foo_list::iterator can be stored inside of
// the Foo object. But at that point you'd be better off using an
// intrusive_list<>:
//
//   map<string,Foo> foo_map;
//   intrusive_list<Foo> foo_list;
//
//   foo_list.push_back(&foo_map["bar"]);
//   foo_list.erase(&foo_map["bar"]); // Yeah!
//
// Note that intrusive_lists come with a few limitations. The primary
// limitation is that the intrusive_link<> base class is not copyable or
// assignable. The result is that STL algorithms which mutate the order of
// iterators, such as reverse() and unique(), will not work by default with
// intrusive_lists. In order to allow these algorithms to work you'll need to
// define swap() and/or operator= for your class.
//
// Another limitation is that the intrusive_list<> structure itself is not
// copyable or assignable since an item/link combination can only exist in one
// intrusive_list<> at a time. This limitation is a result of the link pointers
// for an item being intrusive in the item itself. For example, the following
// will not compile:
//
//   FooList a;
//   FooList b(a); // no copy constructor
//   b = a;        // no assignment operator
//
// The similar STL code does work since the link pointers are external to the
// item:
//
//   list<int*> a;
//   a.push_back(new int);
//   list<int*> b(a);
//   CHECK(a.front() == b.front());
//
// Note that intrusive_list::size() runs in O(N) time.

#ifndef UTIL_GTL_INTRUSIVE_LIST_H_
#define UTIL_GTL_INTRUSIVE_LIST_H_

#include <cstddef>
#include <initializer_list>
#include <utility>

#include "thread_on_boost/absl_headers.h"

namespace gtl {

template <typename T, typename ListID>
class intrusive_list;

template <typename T, typename ListID = void>
class intrusive_link {
 protected:
  // We declare the constructor protected so that only derived types and the
  // befriended list can construct this.
  intrusive_link() : next_(nullptr), prev_(nullptr) {}

#ifndef SWIG
  intrusive_link(const intrusive_link&) = delete;
  intrusive_link& operator=(const intrusive_link&) = delete;
#endif  // SWIG

 private:
  // We befriend the matching list type so that it can manipulate the links
  // while they are kept private from others.
  friend class ::gtl::intrusive_list<T, ListID>;

  // Encapsulates the logic to convert from a link to its derived type.
  T* cast_to_derived() { return static_cast<T*>(this); }
  const T* cast_to_derived() const { return static_cast<const T*>(this); }

  intrusive_link* next_;
  intrusive_link* prev_;
};

template <typename T, typename ListID = void>
class intrusive_list {
  template <typename QualifiedT, typename QualifiedLinkT>
  class iterator_impl;

 public:
  // SWIG doesn't understand 'using' type aliases.
#ifndef SWIG
  using value_type = T;
  using pointer = value_type*;
  using const_pointer = const value_type*;
  using reference = value_type&;
  using const_reference = const value_type&;
  using size_type = size_t;
  using difference_type = ptrdiff_t;

  using link_type = intrusive_link<T, ListID>;
  using iterator = iterator_impl<T, link_type>;
  using const_iterator = iterator_impl<const T, const link_type>;
  using const_reverse_iterator = std::reverse_iterator<const_iterator>;
  using reverse_iterator = std::reverse_iterator<iterator>;
#endif  // SWIG

  intrusive_list() { clear(); }
  // After the move constructor the moved-from list will be empty.
  //
  // NOTE: There is no move assign operator (for now).
  // The reason is that at the moment 'clear()' does not unlink the nodes.
  // It makes is_linked() return true when it should return false.
  // If such node is removed from the list (e.g. from its destructor), or is
  // added to another list - a memory corruption will occur.
  // Admitedly the destructor does not unlink the nodes either, but move-assign
  // will likely make the problem more prominent.
#ifndef SWIG
  intrusive_list(intrusive_list&& src) noexcept {
    clear();
    if (src.empty())
      return;
    sentinel_link_.next_ = src.sentinel_link_.next_;
    sentinel_link_.prev_ = src.sentinel_link_.prev_;
    // Fix head and tail nodes of the list.
    sentinel_link_.prev_->next_ = &sentinel_link_;
    sentinel_link_.next_->prev_ = &sentinel_link_;
    src.clear();
  }
#endif  // SWIG

  // Construct an intrusive_list from a span of element pointers.
  //
  // This is useful for passing a list of known elements to a function that
  // takes an intrusive_list as an argument.
  //
  // Used as:
  //  void Fn(intrusive_list<IntrusiveT> elements);
  //
  //  std::vector<IntrusiveT*> elements = ...;
  //  Fn(intrusive_list<IntrusiveT>(elements));
  explicit intrusive_list(absl::Span<T* const> elements) : intrusive_list() {
    for (T* element : elements) {
      push_back(element);
    }
  }

  // As above, but for an initializer list.
  //
  // Used as:
  //  void Fn(intrusive_list<IntrusiveT> elements);
  //
  //  Fn({&e1, &e2, ...});
  intrusive_list(std::initializer_list<T*> elements)
      : intrusive_list(absl::Span<T* const>(elements)) {}

  iterator begin() { return iterator(sentinel_link_.next_); }
  const_iterator begin() const { return const_iterator(sentinel_link_.next_); }
  iterator end() { return iterator(&sentinel_link_); }
  const_iterator end() const { return const_iterator(&sentinel_link_); }
  reverse_iterator rbegin() { return reverse_iterator(end()); }
  const_reverse_iterator rbegin() const {
    return const_reverse_iterator(end());
  }
  reverse_iterator rend() { return reverse_iterator(begin()); }
  const_reverse_iterator rend() const {
    return const_reverse_iterator(begin());
  }

  bool empty() const { return (sentinel_link_.next_ == &sentinel_link_); }
  // This runs in O(N) time.
  size_type size() const { return std::distance(begin(), end()); }

  reference front() { return *begin(); }
  const_reference front() const { return *begin(); }
  reference back() { return *(--end()); }
  const_reference back() const { return *(--end()); }

  static iterator insert(iterator position, T* obj) {
    return insert_link(position.link(), obj);
  }
  void push_front(T* obj) {
    // This is equivalent to `insert(begin(), obj)`, but faster.
    link_type* const sentinel = &sentinel_link_;
    link_type* const next_link = sentinel->next_;
    link_type* const obj_link = obj;
    obj_link->next_ = next_link;
    obj_link->prev_ = sentinel;
    sentinel->next_ = obj_link;
    next_link->prev_ = obj_link;
  }
  void push_back(T* obj) {
    // This is equivalent to `insert(end(), obj)`, but faster.
    link_type* const sentinel = &sentinel_link_;
    link_type* const obj_link = obj;
    link_type* const prev_link = sentinel->prev_;
    obj_link->next_ = sentinel;
    obj_link->prev_ = prev_link;
    prev_link->next_ = obj_link;
    sentinel->prev_ = obj_link;
  }

  static iterator erase(T* obj) {
    link_type* obj_link = obj;
    // Zero out the next and previous links for the removed item. This will
    // cause any future attempt to remove the item from the list to cause a
    // crash instead of possibly corrupting the list structure.
    link_type* const next_link = std::exchange(obj_link->next_, nullptr);
    link_type* const prev_link = std::exchange(obj_link->prev_, nullptr);
    // Fix up the next and previous links for the previous and next objects.
    next_link->prev_ = prev_link;
    prev_link->next_ = next_link;
    return iterator(next_link);
  }

  static iterator erase(iterator position) {
    return erase(position.operator->());
  }
  void pop_front() {
    // This is equivalent to `erase(begin())`, but faster.
    link_type* sentinel = &sentinel_link_;
    link_type* obj_link = sentinel->next_;
    link_type* next_link = obj_link->next_;
    next_link->prev_ = sentinel;
    sentinel->next_ = std::exchange(obj_link->next_, nullptr);
    obj_link->prev_ = nullptr;
  }
  void pop_back() {
    // This is equivalent to `erase(--end())`, but faster.
    link_type* sentinel = &sentinel_link_;
    link_type* obj_link = sentinel->prev_;
    link_type* prev_link = obj_link->prev_;
    sentinel->prev_ = std::exchange(obj_link->prev_, nullptr);
    prev_link->next_ = sentinel;
    obj_link->next_ = nullptr;
  }

  // Check whether the given element is linked into some list. Note that this
  // does *not* check whether it is linked into a particular list.
  // Also, if clear() is used to clear the containing list, is_linked() will
  // still return true even though obj is no longer in any list.
  static bool is_linked(const T* obj) {
    return obj->link_type::next_ != nullptr;
  }

  ABSL_ATTRIBUTE_REINITIALIZES void clear() {
    sentinel_link_.next_ = sentinel_link_.prev_ = &sentinel_link_;
  }

  void swap(intrusive_list& x) noexcept {
    intrusive_list tmp;
    tmp.splice(tmp.begin(), *this);
    this->splice(this->begin(), x);
    x.splice(x.begin(), tmp);
  }

  void splice(iterator pos, intrusive_list&& src) { splice(pos, src); }

  void splice(iterator pos, intrusive_list& src) {
    splice(pos, src, src.begin(), src.end());
  }

  void splice(iterator pos, intrusive_list& src, iterator i) {
    splice(pos, src, i, std::next(i));
  }

  void splice(iterator pos, intrusive_list& /*src*/, iterator first,
              iterator last) {
    if (first == last)
      return;

    link_type* const last_prev = last.link()->prev_;

    // Remove from the source.
    first.link()->prev_->next_ = last.link();
    last.link()->prev_ = first.link()->prev_;

    // Attach to the destination.
    first.link()->prev_ = pos.link()->prev_;
    pos.link()->prev_->next_ = first.link();
    last_prev->next_ = pos.link();
    pos.link()->prev_ = last_prev;
  }

  // // Prefetches the next or prev element relative to the given position.
  // // NOTE: These are low-level operations and should not be used without
  // // profiling or benchmarking results indicating their appropriateness.
  // friend void GtlIntrusiveListPrefetchNext(const_iterator pos) {
  //   compiler::PrefetchT0(pos.link()->next_);
  // }
  // friend void GtlIntrusiveListPrefetchPrev(const_iterator pos) {
  //   compiler::PrefetchT0(pos.link()->prev_);
  // }

 private:
  static iterator insert_link(link_type* next_link, T* obj) {
    link_type* obj_link = obj;
    obj_link->prev_ = std::exchange(next_link->prev_, obj_link);
    obj_link->prev_->next_ = obj_link;
    obj_link->next_ = next_link;
    return iterator(obj_link);
  }

  // The iterator implementation is parameterized on a potentially qualified
  // variant of T and the matching qualified link type. Essentially, QualifiedT
  // will either be 'T' or 'const T', the latter for a const_iterator.
  template <typename QualifiedT, typename QualifiedLinkT>
  class iterator_impl {
   public:
    using iterator_category = std::bidirectional_iterator_tag;
    using value_type = QualifiedT;
    using difference_type = std::ptrdiff_t;
    using pointer = QualifiedT*;
    using reference = QualifiedT&;

    iterator_impl() : link_(nullptr) {}
    iterator_impl(QualifiedLinkT* link) : link_(link) {}
    iterator_impl(const iterator_impl& x) = default;

    iterator_impl& operator=(const iterator_impl& x) = default;

    // Allow converting and comparing across iterators where the pointer
    // assignment and comparisons (respectively) are allowed.
    template <typename U, typename V>
    iterator_impl(const iterator_impl<U, V>& x) : link_(x.link_) {}
    template <typename U, typename V>
    bool operator==(const iterator_impl<U, V>& x) const {
      return link_ == x.link_;
    }
    template <typename U, typename V>
    bool operator!=(const iterator_impl<U, V>& x) const {
      return link_ != x.link_;
    }

    reference operator*() const { return *operator->(); }
    pointer operator->() const { return link_->cast_to_derived(); }

    QualifiedLinkT* link() const { return link_; }

#ifndef SWIG  // SWIG can't wrap these operator overloads.
    iterator_impl& operator++() {
      link_ = link_->next_;
      return *this;
    }
    iterator_impl operator++(int /*unused*/) {
      iterator_impl tmp = *this;
      ++*this;
      return tmp;
    }
    iterator_impl& operator--() {
      link_ = link_->prev_;
      return *this;
    }
#endif  // SWIG

   private:
    // Ensure iterators can access other iterators node directly.
    template <typename U, typename V>
    friend class iterator_impl;

    QualifiedLinkT* link_;
  };

  // This bare link acts as the sentinel node:
  //  `next_` points to the first element in the list.
  //  `prev_` points to the last element in the list.
  link_type sentinel_link_;

  // These are private and undefined to prevent copying and assigning.
  intrusive_list(const intrusive_list&) = delete;
  void operator=(const intrusive_list&) = delete;
};

}  // namespace gtl

#endif  // UTIL_GTL_INTRUSIVE_LIST_H_
