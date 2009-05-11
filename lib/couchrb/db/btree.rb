require 'pp'

module CouchRB
  module Db

    # Erlang does everything in functional style, so we pass the instance
    # around all the time...
    # Might be easier to refactor that to object oriented style once done.
    module BTreeFunctions
      # % pass in 'nil' for State if a new Btree.
      #   open(State, Fd) ->
      # {ok, #btree{root=State, fd=Fd}}.
      def open(state, fd, options = {})
        bt = BTree.new(fd, state)
        set_options(bt, options)
      end

      def set_options(bt, options = {})
        duplicate = bt.dup

        duplicate.extract_kv  = options[:extract]  if options[:extract]
        duplicate.assemble_kv = options[:assemble] if options[:assemble]
        duplicate.less        = options[:less]     if options[:less]
        duplicate.reduce      = options[:reduce]   if options[:reduce]

        duplicate
      end

      # get_state(#btree{root=Root}) ->
      #     Root.
      def get_state
        root
      end

      # lookup(#btree{root=Root, less=Less}=Bt, Keys) ->
      #     SortedKeys = lists:sort(Less, Keys),
      #     {ok, SortedResults} = lookup(Bt, Root, SortedKeys),
      #     % We want to return the results in the same order as the keys were input
      #     % but we may have changed the order when we sorted. So we need to put the
      #     % order back into the results.
      #     KeyDict = dict:from_list(SortedResults),
      #     [dict:fetch(Key, KeyDict) || Key <- Keys].
      # 
      # lookup(_Bt, nil, Keys) ->
      #     {ok, [{Key, not_found} || Key <- Keys]};
      # lookup(Bt, {Pointer, _Reds}, Keys) ->
      #     {NodeType, NodeList} = get_node(Bt, Pointer),
      #     case NodeType of
      #     kp_node ->
      #         lookup_kpnode(Bt, list_to_tuple(NodeList), 1, Keys, []);
      #     kv_node ->
      #         lookup_kvnode(Bt, list_to_tuple(NodeList), 1, Keys, [])
      #     end.
      #
      # NOTE: changed order of parameters to make this method easier
      #       (bt, root, keys) => (bt, keys, root)
      #       since keys is always required
      def lookup(bt, keys, root = nil)
        root ||= bt.root
#         less = bt.less
#         fail 'no less' unless less

        if root
          pointer, _reds = *root
          fail 'no _reds' unless _reds

          node_type, node_list = *get_node(bt, pointer)

          case node_type
          when :kp_node
            lookup_kpnode(bt, node_list, 1, keys, [])
          when :kv_node
            lookup_kvnode(bt, node_list, 1, keys, [])
          else
            raise 'unknown node_type'
          end
        else
          keys.map{|key| [key, :not_found] }
        end
      end

      # lookup_kvnode(_Bt, _NodeTuple, _LowerBound, [], Output) ->
      #     {ok, lists:reverse(Output)};
      # lookup_kvnode(_Bt, NodeTuple, LowerBound, Keys, Output) when size(NodeTuple) < LowerBound ->
      #     % keys not found
      #     {ok, lists:reverse(Output, [{Key, not_found} || Key <- Keys])};
      # lookup_kvnode(Bt, NodeTuple, LowerBound, [LookupKey | RestLookupKeys], Output) ->
      #     N = find_first_gteq(Bt, NodeTuple, LowerBound, size(NodeTuple), LookupKey),
      #     {Key, Value} = element(N, NodeTuple),
      #     case less(Bt, LookupKey, Key) of
      #     true ->
      #         % LookupKey is less than Key
      #         lookup_kvnode(Bt, NodeTuple, N, RestLookupKeys, [{LookupKey, not_found} | Output]);
      #     false ->
      #         case less(Bt, Key, LookupKey) of
      #         true ->
      #             % LookupKey is greater than Key
      #             lookup_kvnode(Bt, NodeTuple, N+1, RestLookupKeys, [{LookupKey, not_found} | Output]);
      #         false ->
      #             % LookupKey is equal to Key
      #             lookup_kvnode(Bt, NodeTuple, N, RestLookupKeys, [{LookupKey, {ok, assemble(Bt, LookupKey, Value)}} | Output])
      #         end
      #     end.
      def lookup_kvnode(bt, node_tuple, lower_bound, keys, output)
        if keys.empty?
          lists_reverse(output, [])
        else
          if node_tuple.size < lower_bound
            # no keys found
            lists_reverse(output, keys.map{|key| [key, :not_found] })
          else
            lookup_key, *rest_lookup_keys = *keys
            n = find_first_gteq(node_tuple, lower_bound, node_tuple.size, lookup_key)
            key, value = element(n, node_tuple)

            if lookup_key < key
              lookup_kvnode(bt, node_tuple, n, rest_lookup_keys, [[lookup_key, :not_found], *output])
            else
              if key < lookup_key
                lookup_kvnode(bt, node_tuple, n + 1, rest_lookup_keys, [[lookup_key, :not_found], *output])
              else
                lookup_kvnode(bt, node_tuple, n, rest_lookup_keys, [[lookup_key, [ok, [lookup_key, value]]], *output])
              end
            end
          end
        end
      end

      # lookup_kpnode(_Bt, _NodeTuple, _LowerBound, [], Output) ->
      #     {ok, lists:reverse(Output)};
      #     
      # lookup_kpnode(_Bt, NodeTuple, LowerBound, Keys, Output) when size(NodeTuple) < LowerBound ->
      #     {ok, lists:reverse(Output, [{Key, not_found} || Key <- Keys])};
      # 
      # lookup_kpnode(Bt, NodeTuple, LowerBound, [FirstLookupKey | _] = LookupKeys, Output) ->
      #     N = find_first_gteq(Bt, NodeTuple, LowerBound, size(NodeTuple), FirstLookupKey),
      #     {Key, PointerInfo} = element(N, NodeTuple),
      #     SplitFun = fun(LookupKey) -> not less(Bt, Key, LookupKey) end,
      #     case lists:splitwith(SplitFun, LookupKeys) of
      #     {[], GreaterQueries} ->
      #         lookup_kpnode(Bt, NodeTuple, N + 1, GreaterQueries, Output);
      #     {LessEqQueries, GreaterQueries} ->
      #         {ok, Results} = lookup(Bt, PointerInfo, LessEqQueries),
      #         lookup_kpnode(Bt, NodeTuple, N + 1, GreaterQueries, lists:reverse(Results, Output))
      #     end.
      def lookup_kpnode(bt, node_tuple, lower_bound, keys, output)
        if keys.empty?
          lists_reverse(output, [])
        else
          if node_tuple.size < lower_bound
            # no keys found
            lists_reverse(output, keys.map{|key| [key, :not_found] })
          else
            first_lookup_key = keys.first
            n = find_first_gteq(node_tuple, lower_bound, node_tuple.size, first_lookup_key)
            key, pointer_info = element(n, node_tuple)
            # THINK: should just use >= here?
            less_eq_queries, greater_queries = lookup_keys.partition{|lookup_key| !(key < lookup_key) }
            if less_eq_queries.empty?
              lookup_kpnode(bt, node_tuple, n + 1, greater_queries, output)
            else
              results = lookup(bt, pointer_info, less_eq_queries)
              lookup_kpnode(bt, node_tuple, n + 1, greater_queries, lists_reverse(results, output))
            end
          end
        end
      end


      # fold(Bt, Dir, Fun, Acc) ->
      #     {_ContinueFlag, Acc2} = stream_node(Bt, [], Bt#btree.root, nil, Dir, convert_fun_arity(Fun), Acc),
      #     {ok, Acc2}.
      #
      # fold(Bt, Key, Dir, Fun, Acc) ->
      #     {_ContinueFlag, Acc2} = stream_node(Bt, [], Bt#btree.root, Key, Dir, convert_fun_arity(Fun), Acc),
      #     {ok, Acc2}.
      def fold(bt, dir, acc, key = nil, &fun)
        # p :fold => { :dir => dir, :acc => acc, :key => key }
        fun = convert_fun_arity(&fun)

        _continue_flag, acc2 = stream_node(bt, [], bt.root, acc, dir, key, &fun)
        return :ok, acc2
      end

      # foldl(Bt, Fun, Acc) ->
      #     fold(Bt, fwd, Fun, Acc).
      #
      # foldl(Bt, Key, Fun, Acc) ->
      #     fold(Bt, Key, fwd, Fun, Acc).
      def foldl(bt,       acc, key = nil, &fun)
        fold(   bt, :fwd, acc, key,       &fun)
      end

      # foldr(Bt, Fun, Acc) ->
      #     fold(Bt, rev, Fun, Acc).
      #
      # foldr(Bt, Key, Fun, Acc) ->
      #     fold(Bt, Key, rev, Fun, Acc).
      def foldr(bt,       acc, key = nil, &fun)
        fold(   bt, :rev, acc, key,       &fun)
      end

      # add(Bt, InsertKeyValues) ->
      #     add_remove(Bt, InsertKeyValues, []).
      #
      # add_remove(Bt, InsertKeyValues, RemoveKeys) ->
      #     {ok, [], Bt2} = query_modify(Bt, [], InsertKeyValues, RemoveKeys),
      #     {ok, Bt2}.
      def add_remove(bt, insert_key_values, remove_keys = [])
        pp :add_remove => {:bt => bt, :insert_key_values => insert_key_values, :remove_keys => remove_keys}
        left, bt2 = query_modify(bt, [], insert_key_values, remove_keys)
        fail "left is not []" unless left == []
        bt2
      end

      # query_modify(Bt, LookupKeys, InsertValues, RemoveKeys) ->
      #     #btree{root=Root} = Bt,
      #     InsertActions = lists:map(
      #         fun(KeyValue) ->
      #             {Key, Value} = extract(Bt, KeyValue),
      #             {insert, Key, Value}
      #         end, InsertValues),
      #     RemoveActions = [{remove, Key, nil} || Key <- RemoveKeys],
      #     FetchActions = [{fetch, Key, nil} || Key <- LookupKeys],
      #     SortFun =
      #         fun({OpA, A, _}, {OpB, B, _}) ->
      #             case less(Bt, A, B) of
      #             true -> true;
      #             false ->
      #                 case less(Bt, B, A) of
      #                 true -> false;
      #                 false ->
      #                     % A and B are equal, sort by op.
      #                     op_order(OpA) < op_order(OpB)
      #                 end
      #             end
      #         end,
      #     Actions = lists:sort(SortFun, lists:append([InsertActions, RemoveActions, FetchActions])),
      #     {ok, KeyPointers, QueryResults, Bt2} = modify_node(Bt, Root, Actions, []),
      #     {ok, NewRoot, Bt3} = complete_root(Bt2, KeyPointers),
      #     {ok, QueryResults, Bt3#btree{root=NewRoot}}.

      def query_modify(bt, lookup_keys, insert_values, remove_keys)
        pp :query_modify => {:bt => bt, :lookup_keys => lookup_keys, :insert_values => insert_values, :remove_keys => remove_keys}
        root = bt.root # FIXME: is that really what it should do?

        insert_actions = insert_values.map{|key_value| [:insert, *key_value] }
        remove_actions = remove_keys.map{|key|         [:remove, key, nil  ] }
        fetch_actions  = lookup_keys.map{|key|         [:fetch,  key, nil  ] }

        unsorted_actions = (insert_actions + remove_actions + fetch_actions)
        actions = unsorted_actions.sort{|(op_a, a, _), (op_b, b, _)|
          if less(a, b)
            1
          elsif less(b, a)
            -1
          else
            op_order(op_a) <=> op_order(op_b)
          end
        }

        key_pointers, query_results, bt2 = modify_node(bt, root, actions, [])
        p :key_pointers => key_pointers, :query_results => query_results, :bt2 => bt2
        new_root, bt3 = complete_root(bt2, *key_pointers)
        bt4 = bt3.dup # seems like erlang does this
        bt4.root = new_root
        return query_results, bt4
      end

      def op_order(op)
        case op
        when :fetch;  1
        when :remove; 2
        when :insert; 3
        else; raise("Invalid action op: %p" % op)
        end
      end

      def modify_node(bt, root_pointer_info, actions, query_output)
        pp :modify_node => {:bt => bt, :root_pointer_info => root_pointer_info, :actions => actions, :query_output => query_output}
        # p caller

        if root_pointer_info == nil
          node_type = :kv_node
          node_list = []
        else
          pointer, _reds = *root_pointer_info
          # fail 'no reds' unless _reds
          node_type, node_list = *get_node(bt, pointer)
        end

        p :node_type => node_type, :node_list => node_list

        case node_type
        when :kp_node
          result = modify_kpnode(bt, node_list, 1, actions, [], query_output)
          new_node_list, query_output2, bt2 = *result
        when :kv_node
          result = modify_kvnode(bt, node_list, 1, actions, [], query_output)
          new_node_list, query_output2, bt2 = *result
        else
          raise('No such node_type: %p' % node_type)
        end

        fail 'no bt2' unless bt2
        pp :result => result, :new_node_list => new_node_list, :query_output2 => query_output2, :bt2 => bt2
        p caller

        case new_node_list
        when [] # no nodes remain
          return [], query_output2, bt2
        when node_list # nothing changed
          last_key, last_value = element(node_list.size, node_list)
          return [last_key, root_pointer_info], query_output2, bt2
        else
          result_list, bt3 = write_node(bt2, node_type, new_node_list)
          fail 'no bt3' unless bt3
          return result_list, query_output2, bt3
        end
      end

      def get_node(bt, pos)
        pp :get_node => {:pos => pos}
        node_type, node_list = *bt.fd.pread_term(pos)
      end

      # modify_kpnode(Bt, {}, _LowerBound, Actions, [], QueryOutput) ->
      #     modify_node(Bt, nil, Actions, QueryOutput);
      # modify_kpnode(Bt, NodeTuple, LowerBound, [], ResultNode, QueryOutput) ->
      #     {ok, lists:reverse(ResultNode, bounded_tuple_to_list(NodeTuple, LowerBound,
      #             size(NodeTuple),  [])), QueryOutput, Bt};
      # modify_kpnode(Bt, NodeTuple, LowerBound,
      #         [{_, FirstActionKey, _}|_]=Actions, ResultNode, QueryOutput) ->
      #     N = find_first_gteq(Bt, NodeTuple, LowerBound, size(NodeTuple), FirstActionKey),
      #     case N == size(NodeTuple) of
      #     true  ->
      #         % perform remaining actions on last node
      #         {_, PointerInfo} = element(size(NodeTuple), NodeTuple),
      #         {ok, ChildKPs, QueryOutput2, Bt2} =
      #             modify_node(Bt, PointerInfo, Actions, QueryOutput),
      #         NodeList = lists:reverse(ResultNode, bounded_tuple_to_list(NodeTuple, LowerBound,
      #             size(NodeTuple) - 1, ChildKPs)),
      #         {ok, NodeList, QueryOutput2, Bt2};
      #     false ->
      #         {NodeKey, PointerInfo} = element(N, NodeTuple),
      #         SplitFun = fun({_ActionType, ActionKey, _ActionValue}) ->
      #                 not less(Bt, NodeKey, ActionKey)
      #             end,
      #         {LessEqQueries, GreaterQueries} = lists:splitwith(SplitFun, Actions),
      #         {ok, ChildKPs, QueryOutput2, Bt2} =
      #                 modify_node(Bt, PointerInfo, LessEqQueries, QueryOutput),
      #         ResultNode2 = lists:reverse(ChildKPs, bounded_tuple_to_revlist(NodeTuple,
      #                 LowerBound, N - 1, ResultNode)),
      #         modify_kpnode(Bt2, NodeTuple, N+1, GreaterQueries, ResultNode2, QueryOutput2)
      #     end.
      def modify_kpnode(bt, node_tuple, lower_bound, actions, result_node, query_output)
        pp :modify_kpnode => { :bt => bt, :node_tuple => node_tuple, :lower_bound => lower_bound, :actions => actions, :result_node => result_node }
        # p caller

        if node_tuple.empty? and result_node.empty?
          modify_node(bt, nil, actions, query_output)
        else
          if actions.empty?
            reversed = lists_reverse(result_node,
                                     bounded_tuple_to_list(node_tuple,
                                                            lower_bound,
                                                            node_tuple.size,
                                                            []))
            return reversed, query_output, bt
          else
            first_action_key = actions.first[1]
            n = find_first_gteq(node_tuple, lower_bound, node_tuple.size, first_action_key)

            if n == node_tuple.size
              # perform remaining actions on last node
              _, pointer_info = element(node_tuple.size, node_tuple)

              child_kps, query_output2, bt2 =
                modify_node(bt, pointer_info, actions, query_output)
              fail 'no bt2' unless bt2
              node_list = lists_reverse(result_node,
                                        bounded_tuple_to_list(node_tuple,
                                                               lower_bound,
                                                               node_tuple.size - 1,
                                                               child_kps))

              return node_list, query_output2, bt2
            else
              node_key, pointer_info = element(n, node_tuple)
              p :node_key => node_key, :pointer_info => pointer_info

              less_eq_queries, greater_queries =
                actions.partition{|(action_type, action_key, action_value)|
                  action_key <= node_key }

              child_kps, query_output2, bt2 = modify_node(bt, pointer_info, less_eq_queries, query_output)
              fail 'no bt2' unless bt2
              result_node2 = bounded_tuple_to_revlist(node_tuple, lower_bound, n - 1, result_node)
              modify_kpnode(bt2, node_tuple, n + 1, greater_queries, result_node2, query_output2)
            end
          end
        end
      end

      # modify_kvnode(Bt, NodeTuple, LowerBound, [], ResultNode, QueryOutput) ->
      #     {ok, lists:reverse(ResultNode, bounded_tuple_to_list(NodeTuple, LowerBound, size(NodeTuple), [])), QueryOutput, Bt};
      #
      # modify_kvnode(Bt, NodeTuple, LowerBound, [{ActionType, ActionKey, ActionValue} | RestActions], ResultNode, QueryOutput) when LowerBound > size(NodeTuple) ->
      #     case ActionType of
      #     insert ->
      #         modify_kvnode(Bt, NodeTuple, LowerBound, RestActions, [{ActionKey, ActionValue} | ResultNode], QueryOutput);
      #     remove ->
      #         % just drop the action
      #         modify_kvnode(Bt, NodeTuple, LowerBound, RestActions, ResultNode, QueryOutput);
      #     fetch ->
      #         % the key/value must not exist in the tree
      #         modify_kvnode(Bt, NodeTuple, LowerBound, RestActions, ResultNode, [{not_found, {ActionKey, nil}} | QueryOutput])
      #     end;
      #
      # modify_kvnode(Bt, NodeTuple, LowerBound, [{ActionType, ActionKey, ActionValue} | RestActions], AccNode, QueryOutput) ->
      #     N = find_first_gteq(Bt, NodeTuple, LowerBound, size(NodeTuple), ActionKey),
      #     {Key, Value} = element(N, NodeTuple),
      #     ResultNode =  bounded_tuple_to_revlist(NodeTuple, LowerBound, N - 1, AccNode),
      #     case less(Bt, ActionKey, Key) of
      #     true ->
      #         case ActionType of
      #         insert ->
      #             % ActionKey is less than the Key, so insert
      #             modify_kvnode(Bt, NodeTuple, N, RestActions, [{ActionKey, ActionValue} | ResultNode], QueryOutput);
      #         remove ->
      #             % ActionKey is less than the Key, just drop the action
      #             modify_kvnode(Bt, NodeTuple, N, RestActions, ResultNode, QueryOutput);
      #         fetch ->
      #             % ActionKey is less than the Key, the key/value must not exist in the tree
      #             modify_kvnode(Bt, NodeTuple, N, RestActions, ResultNode, [{not_found, {ActionKey, nil}} | QueryOutput])
      #         end;
      #     false ->
      #         % ActionKey and Key are maybe equal.
      #         case less(Bt, Key, ActionKey) of
      #         false ->
      #             case ActionType of
      #             insert ->
      #                 modify_kvnode(Bt, NodeTuple, N+1, RestActions, [{ActionKey, ActionValue} | ResultNode], QueryOutput);
      #             remove ->
      #                 modify_kvnode(Bt, NodeTuple, N+1, RestActions, ResultNode, QueryOutput);
      #             fetch ->
      #                 % ActionKey is equal to the Key, insert into the QueryOutput, but re-process the node
      #                 % since an identical action key can follow it.
      #                 modify_kvnode(Bt, NodeTuple, N, RestActions, ResultNode, [{ok, assemble(Bt, Key, Value)} | QueryOutput])
      #             end;
      #         true ->
      #             modify_kvnode(Bt, NodeTuple, N + 1, [{ActionType, ActionKey, ActionValue} | RestActions], [{Key, Value} | ResultNode], QueryOutput)
      #         end
      #     end.
      #
      #
      # this thing is way too huge, seriously. used to be three functions in
      # erlang but we don't have pattern matching.
      # and then it's still recursive too...
      def modify_kvnode(bt, node_tuple, lower_bound, actions, result_node, query_output)
        pp :modify_kvnode => { :bt => bt, :node_tuple => node_tuple, :lower_bound => lower_bound, :actions => actions, :result_node => result_node, :query_output => query_output, }

        if actions.empty?
          # p [result_node, bounded_tuple_to_list(node_tuple, lower_bound, node_tuple.size, [])]
          reversed = lists_reverse(result_node, bounded_tuple_to_list(node_tuple, lower_bound, node_tuple.size, []))
          return reversed, query_output, bt
        else
          action, *rest_actions = actions
          # p :action => action, :rest_actions => rest_actions
          action_type, action_key, action_value = *action
          # p :action_type => action_type, :action_key => action_key, :action_value => action_value

          if lower_bound > node_tuple.size
            p :lower_bound => lower_bound, :node_tuple_size => node_tuple.size

            case action_type
            when :insert
              p 'lower_bound > node_tuple.size, so insert'
              modify_kvnode(bt, node_tuple, lower_bound, rest_actions, [[action_key, action_value], *result_node], query_output)
            when :remove
              p 'lower_bound > node_tuple.size, just drop the action'
              modify_kvnode(bt, node_tuple, lower_bound, rest_actions, result_node, query_output)
            when :fetch
              p 'the key/value must not exist in the tree'
              modify_kvnode(bt, node_tuple, lower_bound, rest_actions, result_node, [[:not_found, [action_key, nil]], *query_output])
            else
              raise 'unknown action_type'
            end
          else
            acc_node = result_node

            n = find_first_gteq(node_tuple, lower_bound, node_tuple.size, action_key)
            key, value = element(n, node_tuple)
            result_node = bounded_tuple_to_list(node_tuple, lower_bound, n - 1, acc_node)

            if action_key < key
              p :action_key => action_key, :key => key
              case action_type
              when :insert
                p 'action_key is less than the key, so insert'
                modify_kvnode(bt, node_tuple, n, rest_actions, [[action_key, action_value], *result_node], query_output)
              when :remove
                p 'action_key is less than the key, just drop the action'
                modify_kvnode(bt, node_tuple, n, rest_actions, result_node, query_output)
              when :fetch
                p 'action_key is less than the key, the key/value must not exist in the tree'
                modify_kvnode(bt, node_tuple, n, rest_actions, [[action_key, action_value], *result_node], query_output)
              else
                raise 'unknown action_type'
              end
            else
              if key < action_key
                case action_type
                when :insert
                  # p 'action_key is equal to key, so insert'
                  modify_kvnode(bt, node_tuple, n + 1, rest_actions, [[action_key, action_value], *result_node], query_output)
                when :remove
                  # p 'action_key is equal to key, rmeove'
                  modify_kvnode(bt, node_tuple, n + 1, rest_actions, result_node, query_output)
                when :fetch
                  # p 'action_key is equal to key, insert into query_output and reprocess'
                  # action_key is equal to key, insert into the query_output,
                  # but re-process the node since an identical action key can
                  # follow it.
                  modify_kvnode(bt, node_tuple, n, rest_actions, result_node, [[:ok, [key, value]], *query_output])
                else
                  raise 'unknown action_type'
                end
              else
                modify_kvnode(bt, node_tuple, n + 1, [[action_type, action_key, action_value], *rest_actions], [[key, value], *result_node], query_output)
              end
            end
          end
        end
      end


      # write_node(Bt, NodeType, NodeList) ->
      #     % split up nodes into smaller sizes
      #     NodeListList = chunkify(Bt, NodeList),
      #     % now write out each chunk and return the KeyPointer pairs for those nodes
      #     ResultList = [
      #         begin
      #             {ok, Pointer} = couch_file:append_term(Bt#btree.fd, {NodeType, ANodeList}),
      #             {LastKey, _} = lists:last(ANodeList),
      #             {LastKey, {Pointer, reduce_node(Bt, NodeType, ANodeList)}}
      #         end
      #     ||
      #         ANodeList <- NodeListList
      #     ],
      #     {ok, ResultList, Bt}.
      #
      # split up nodes into smaller sizes and write them out each chunk,
      # returning the key_pointer pairs for those nodes
      def write_node(bt, node_type, node_list)
        pp :write_node => {:bt => bt, :node_type => node_type, :node_list => node_list}
        node_list_list = [node_list]

        result_list = node_list_list.map{|a_node_list|
          pointer = bt.fd.append_term([node_type, a_node_list])
          last_key, _ = *lists_last(a_node_list)
          [last_key, [pointer, reduce_node(bt, node_type, a_node_list)]]
        }

        return result_list, bt
      end

      def chunkify(bt, list)
        return list
        return [] if list.empty?

        chunks = [[]]
        so_far = 0

        list.each do |node|
          term = CouchRB::Db::Term.dump(node)
          size = term.size

          if so_far > BTree::CHUNK_THRESHOLD
            chunks << [node]
          else
            chunks.last << node
          end
        end

        chunks
      end

      # reduce_node(#btree{reduce=nil}, _NodeType, _NodeList) ->
      #     [];
      # reduce_node(#btree{reduce=R}, kp_node, NodeList) ->
      #     R(rereduce, [Red || {_K, {_P, Red}} <- NodeList]);
      # reduce_node(#btree{reduce=R}=Bt, kv_node, NodeList) ->
      #     R(reduce, [assemble(Bt, K, V) || {K, V} <- NodeList]).
      def reduce_node(bt, node_type, node_list)
        # pp :reduce_node => {:bt => bt, :node_type => node_type, :node_list => node_list}
        return [] unless bt.reduce

        case node_type
        when :kp_node
          nodes = node_list.map{|(_k, (_p, red))|
            # p :rereduce => {:_k => _k, :_p => _p, :red => red}
            red
          }
          bt.reduce.call(:rereduce, nodes)
        when :kv_node
          nodes = node_list.map{|(key, value)|
            # p :reduce => {:key => key, :value => value}
            [key, value]
          }
          bt.reduce.call(:reduce, nodes)
        else
          raise 'invalid node_type: %p' % [node_type]
        end
      end

      # +dir+ is the direction (:fwd || :rev)
      def stream_node(bt, reds, pointer_info, acc, dir, start_key = nil, &fun)
        pp :stream_node => {:bt => bt, :reds => reds, :pointer_info => pointer_info, :start_key => start_key, :dir => dir, :acc => acc}
        pointer, _reds  = pointer_info
        # p :stream_node => { :reds => reds, :pointer_info => pointer_info, :pointer => pointer, :_reds => _reds, :start_key => start_key, :dir => dir, :acc => acc}

        return :ok, acc if pointer_info.nil?

        node_type, node_list = get_node(bt, pointer)
        p :node_type => node_type, :node_list => node_list
        adjusted_list = adjust_dir(dir, node_list)
        # p :adjusted_list => adjusted_list

        if start_key
          case node_type
          when :kp_node; stream_kp_node(bt, reds, adjusted_list, acc, dir, start_key, &fun)
          when :kv_node; stream_kv_node(bt, reds, adjusted_list, acc, dir, start_key, &fun)
          else
            raise "invalid start_key: %p" % [start_key]
          end
        else
          case node_type
          when :kp_node; stream_kp_node(bt, reds, adjusted_list, acc, dir, &fun)
          when :kv_node; stream_kv_node2(bt, reds, [], adjusted_list, acc, dir, &fun)
          else
            raise "No such node_type"
          end
        end
      end

      # stream_kp_node(_Bt, _Reds, [], _Dir, _Fun, Acc) ->
      #     {ok, Acc};
      #
      # stream_kp_node(Bt, Reds, [{_Key, {Pointer, Red}} | Rest], Dir, Fun, Acc) ->
      #     case stream_node(Bt, Reds, {Pointer, Red}, Dir, Fun, Acc) of
      #     {ok, Acc2} ->
      #         stream_kp_node(Bt, [Red | Reds], Rest, Dir, Fun, Acc2);
      #     {stop, Acc2} ->
      #         {stop, Acc2}
      #     end.
      #
      # stream_kp_node(Bt, Reds, KPs, StartKey, Dir, Fun, Acc) ->
      #     {NewReds, NodesToStream} =
      #     case Dir of
      #     fwd ->
      #         % drop all nodes sorting before the key
      #         drop_nodes(Bt, Reds, StartKey, KPs);
      #     rev ->
      #         % keep all nodes sorting before the key, AND the first node to sort after
      #         RevKPs = lists:reverse(KPs),
      #         case lists:splitwith(fun({Key, _Pointer}) -> less(Bt, Key, StartKey) end, RevKPs) of
      #         {_RevBefore, []} ->
      #             % everything sorts before it
      #             {Reds, KPs};
      #         {RevBefore, [FirstAfter | Drop]} ->
      #             {[Red || {_K,{_P,Red}} <- Drop] ++ Reds,
      #                 [FirstAfter | lists:reverse(RevBefore)]}
      #         end
      #     end,
      #     case NodesToStream of
      #     [] ->
      #         {ok, Acc};
      #     [{_Key, {Pointer, Red}} | Rest] ->
      #         case stream_node(Bt, NewReds, {Pointer, Red}, StartKey, Dir, Fun, Acc) of
      #         {ok, Acc2} ->
      #             stream_kp_node(Bt, [Red | NewReds], Rest, Dir, Fun, Acc2);
      #         {stop, Acc2} ->
      #             {stop, Acc2}
      #         end
      #     end.
      #
      # stream_kp_node(Bt, Reds, [],                                        Dir, Fun, Acc) ->
      # stream_kp_node(Bt, Reds, [{_Key, {Pointer, Red}} | Rest],           Dir, Fun, Acc) ->
      # stream_kp_node(Bt, Reds, KPs,                             StartKey, Dir, Fun, Acc) ->
      def stream_kp_node(bt, reds, kps, acc, dir, start_key = nil, &fun)
        pp :stream_kp_node => {:bt => bt, :reds => reds, :acc => acc, :dir => dir, :start_key => start_key}
        p caller
        return :ok, acc if kps.empty?

        unless start_key
          p :kps => kps
          _key, (pointer, red), *rest = *kps
          p :_key => _key, :pointer => pointer, :red => red, :rest => rest
          result, acc2 = stream_node(bt, reds, [pointer, red], acc, dir, &fun)
          p :result => result, :acc2 => acc2

          case result
          when :ok
            stream_kp_node(bt, [red, *reds], rest, acc2, dir, &fun)
          when :stop
            return :stop, acc2
          else
            raise 'invalid result: %p' % [result]
          end
        else
          case dir
          when :fwd
            new_reds, nodes_to_stream = drop_nodes(bt, reds, start_key, kps)
          when :rev
            # keep all nodes sorting before the key, AND the first node to sort after
            rev_kps = kps.reverse
            revs_before, revs_after = rev_kps.partition{|(key, pointer)| key < start_key }
            if revs_after.empty?
              # everything sorts before it
              new_reds, nodes_to_stream = reds, kps
            else
              raise 'complexity overload! implement me, please?'
              #         {RevBefore, [FirstAfter | Drop]} ->
              #             {[Red || {_K,{_P,Red}} <- Drop] ++ Reds,
              #                 [FirstAfter | lists:reverse(RevBefore)]}
            end

            if nodes_to_stream.empty?
              return :ok, acc
            else
              (key, (pointer, red)), *rest = *nodes_to_stream

              result, acc2 = stream_node(bt, new_reds, [pointer, red], acc, dir, start_key, &fun)

              case result
              when :ok
                stream_kp_node(bt, [red, *new_reds], rest, acc2, dir, &fun)
              when :stop
                [:stop, acc2]
              else
                raise 'invalid result: %p' % [result]
              end
            end
          else
            raise 'invalid dir: %p' % [dir]
          end
        end

      #     case NodesToStream of
      #     [] ->
      #         {ok, Acc};
      #     [{_Key, {Pointer, Red}} | Rest] ->
      #         case stream_node(Bt, NewReds, {Pointer, Red}, StartKey, Dir, Fun, Acc) of
      #         {ok, Acc2} ->
      #             stream_kp_node(Bt, [Red | NewReds], Rest, Dir, Fun, Acc2);
      #         {stop, Acc2} ->
      #             {stop, Acc2}
      #         end
      #     end.
      end

      # stream_kv_node(Bt, Reds, KVs, StartKey, Dir, Fun, Acc) ->
      #     DropFun =
      #     case Dir of
      #     fwd ->
      #         fun({Key, _}) -> less(Bt, Key, StartKey) end;
      #     rev ->
      #         fun({Key, _}) -> less(Bt, StartKey, Key) end
      #     end,
      #     {LTKVs, GTEKVs} = lists:splitwith(DropFun, KVs),
      #     AssembleLTKVs = [assemble(Bt,K,V) || {K,V} <- LTKVs],
      #     stream_kv_node2(Bt, Reds, AssembleLTKVs, GTEKVs, Dir, Fun, Acc).
      #
      # stream_kv_node2(_Bt, _Reds, _PrevKVs, [], _Dir, _Fun, Acc) ->
      #     {ok, Acc};
      #
      # stream_kv_node2(Bt, Reds, PrevKVs, [{K,V} | RestKVs], Dir, Fun, Acc) ->
      #     AssembledKV = assemble(Bt, K, V),
      #     case Fun(AssembledKV, {PrevKVs, Reds}, Acc) of
      #     {ok, Acc2} ->
      #         stream_kv_node2(Bt, Reds, [AssembledKV | PrevKVs], RestKVs, Dir, Fun, Acc2);
      #     {stop, Acc2} ->
      #         {stop, Acc2}
      #     end.
      def stream_kv_node(bt, reds, kvs, start_key, acc, dir, &fun)
        pp :stream_kv_node => {:bt => bt, :reds => reds, :kvs => kvs, :start_key => start_key, :dir => dir, :acc => acc}
        return :ok, acc if kvs.empty?
        raise 'not implemented'
      end

      # stream_kv_node2(_Bt, _Reds, _PrevKVs, [], _Dir, _Fun, Acc) ->
      #     {ok, Acc};
      # stream_kv_node2(Bt, Reds, PrevKVs, [{K,V} | RestKVs], Dir, Fun, Acc) ->
      #     AssembledKV = assemble(Bt, K, V),
      #     case Fun(AssembledKV, {PrevKVs, Reds}, Acc) of
      #     {ok, Acc2} ->
      #         stream_kv_node2(Bt, Reds, [AssembledKV | PrevKVs], RestKVs, Dir, Fun, Acc2);
      #     {stop, Acc2} ->
      #         {stop, Acc2}
      #     end.
      def stream_kv_node2(bt, reds, prev_kvs, kvs, acc, dir, &fun)
        pp :stream_kv_node2 => {:bt => bt, :reds => reds, :prev_kvs => prev_kvs, :kvs => kvs, :acc => acc, :dir => dir}
        kvs = [*kvs]
        return acc if kvs.empty?

        previous = prev_kvs.dup

        kvs.each do |key, value|
          kv = [key, value]
          previous = kv

          status, acc = yield(kv, [previous, reds], acc)
#           previous.unshift(kv)

          case status
          when :ok
          when :stop
            return :stop, acc
          else
            raise "invalid return status"
          end
        end

        return :ok, acc
      end

      # final_reduce(#btree{reduce=Reduce}, Val) ->
      #     final_reduce(Reduce, Val);
      #
      # final_reduce(Reduce, {[], []}) ->
      #     Reduce(reduce, []);
      #
      # final_reduce(_Bt, {[], [Red]}) ->
      #     Red;
      #
      # final_reduce(Reduce, {[], Reductions}) ->
      #     Reduce(rereduce, Reductions);
      #
      # final_reduce(Reduce, {KVs, Reductions}) ->
      #     Red = Reduce(reduce, KVs),
      #     final_reduce(Reduce, {[], [Red | Reductions]}).
      def final_reduce(bt, leading_reductions, &reduce)
        pp :final_reduce => {:bt => bt, :leading_reductions => leading_reductions}
        if block_given?
          reduced = leading_reductions.map{|kv| yield(:reduce, kv) }
          yield(:rereduce, reduced)
        else
          final_reduce(bt, leading_reductions, &bt.reduce)
        end
      end

      # complete_root(Bt, []) ->
      #     {ok, nil, Bt};
      # complete_root(Bt, [{_Key, PointerInfo}])->
      #     {ok, PointerInfo, Bt};
      # complete_root(Bt, KPs) ->
      #     {ok, ResultKeyPointers, Bt2} = write_node(Bt, kp_node, KPs),
      #     complete_root(Bt2, ResultKeyPointers).
      #
      # OK, this makes one wish for pattern matching :)
      def complete_root(bt, *kps)
        pp :complete_root => {:bt => bt, :kps => kps}
        if kps.empty?
          return bt.root, bt
        else
          if kps.size == 1
            _key, pointer_info = *kps[0]
            return pointer_info, bt
          else
            result_key_pointers, bt2 = write_node(bt, :kp_node, kps)
            complete_root(bt2, *result_key_pointers)
          end
        end
      end

      # % wraps a 2 arity function with the proper 3 arity function
      # convert_fun_arity(Fun) when is_function(Fun, 2) ->
      #     fun(KV, _Reds, AccIn) -> Fun(KV, AccIn) end;
      # convert_fun_arity(Fun) when is_function(Fun, 3) ->
      #     Fun.    % Already arity 3
      def convert_fun_arity(&fun)
        if fun.arity == 2
          lambda{|kv, _reds, acc_in| fun[kv, acc_in] }
        else
          fun
        end
      end

      # mostly crazy utility methods needed in erlang, we will eventually
      # refactor that.

      def extract(key_value)
        return *key_value
      end

      def assemble(key, value)
        return key, value
      end

      def less(a, b)
        a < b
      end

      def lists_last(list)
        list.last
      end

      # erlang seriously starts indexing at 1...
      def element(n, list)
        list[n - 1]
      end

      def lists_reverse(head, tail)
        head.reverse.concat(tail)
      end

      def bounded_tuple_to_revlist(tuple, from, to, tail)
        if from > to
          tail
        else
          bounded_tuple_to_revlist(tuple, from + 1, to, [element(from, tuple), *tail])
        end
      end

      def bounded_tuple_to_list(tuple, from, to, tail)
        bounded_tuple_to_list2(tuple, from, to, [], tail)
      end

      # bounded_tuple_to_list2(_Tuple, Start, End, Acc, Tail) when Start > End ->
      #   lists:reverse(Acc, Tail);
      # bounded_tuple_to_list2(Tuple, Start, End, Acc, Tail) ->
      #   bounded_tuple_to_list2(Tuple, Start + 1, End, [element(Start, Tuple) | Acc], Tail).
      def bounded_tuple_to_list2(tuple, from, to, acc, tail)
        if from > to
          lists_reverse(acc, tail)
        else
          bounded_tuple_to_list2(tuple, from + 1, to, [element(from, tuple), *acc], tail)
        end
      end

      # find_first_gteq(_Bt, _Tuple, Start, End, _Key) when Start == End ->
      #     End;
      # find_first_gteq(Bt, Tuple, Start, End, Key) ->
      #     Mid = Start + ((End - Start) div 2),
      #     {TupleKey, _} = element(Mid, Tuple),
      #     case less(Bt, TupleKey, Key) of
      #     true ->
      #         find_first_gteq(Bt, Tuple, Mid+1, End, Key);
      #     false ->
      #         find_first_gteq(Bt, Tuple, Start, Mid, Key)
      #     end.
      def find_first_gteq(tuple, from, to, key)
        return to if from == to

        mid = from + ((to - from) / 2)
        # p :find_first_gteq => {:tuple => tuple, :from => from, :to => to, :key => key, :mid => mid}
        tuple_key, _ = element(mid, tuple)

        if tuple_key < key
          find_first_gteq(tuple, mid + 1, to, key)
        else
          find_first_gteq(tuple, from, mid, key)
        end
      end

      # adjust_dir(fwd, List) ->
      #     List;
      # adjust_dir(rev, List) ->
      #     lists:reverse(List).
      def adjust_dir(dir, list)
        case dir
        when :fwd; list
        when :rev; list.reverse
        else
          raise "invalid dir: %p" % [dir]
        end
      end
    end

    # -record(btree,
    #     {fd,
    #     root,
    #     extract_kv = fun({Key, Value}) -> {Key, Value} end,
    #     assemble_kv =  fun(Key, Value) -> {Key, Value} end,
    #     less = fun(A, B) -> A < B end,
    #     reduce = nil
    #     }).
    class BTree
      attr_accessor :fd, :root, :extract_kv, :assemble_kv, :reduce, :less

      CHUNK_THRESHOLD = 1279 # hope that is correct...

      def initialize(fd, root = nil, extract_kv = nil, assemble_kv = nil, reduce = nil)
        self.fd = fd
        self.root = root
        self.extract_kv = extract_kv || lambda{|(key, value)| [key, value] }
        self.assemble_kv = assemble_kv || lambda{|key, value| [key, value] }
        self.reduce = reduce
      end

      def inspect
        "#<BTree fd.path=%p root=%p>" % [fd.path, root]
      end

      extend BTreeFunctions
    end
  end
end

#original CouchDB btree tests

BTree = CouchRB::Db::BTree

require 'lib/couchrb/db/file'

# test_keys(Btree, List) ->
#     FoldFun =
#     fun(Element, [HAcc|TAcc]) ->
#             Element = HAcc, % must match
#             {ok, TAcc}
#         end,
#     Sorted = lists:sort(List),
#     {ok, []} = foldl(Btree, FoldFun, Sorted),
#     {ok, []} = foldr(Btree, FoldFun, lists:reverse(Sorted)),
#
#     test_lookup(Btree, List).
def test_keys(bt, list)
  fold_fun = lambda{|element, (hacc, tacc), acc|
    p :element => element, :hacc => hacc, :tacc => tacc, :acc => acc
    fail("must match") unless element == hacc
    [:ok, tacc]
  }

  sorted = list.sort
  foldl_result = BTree.foldl(bt, sorted, &fold_fun)
  fail("failed foldl") unless [:ok, []] == foldl_result

  foldr_result = BTree.foldr(bt, sorted.reverse, &fold_fun)
  fail("failed foldr") unless [:ok, []] == foldr_result
end

# removes each key one at a time from the btree
#
# test_remove(Btree, []) ->
#     {ok, Btree};
# test_remove(Btree, [{Key, _Value} | Rest]) ->
#     {ok, Btree2} = add_remove(Btree,[], [Key]),
#     test_remove(Btree2, Rest).
def test_remove(bt, kvs)
  kvs.each do |(key, value)|
    bt = BTree.add_remove(bt, [], [key])
  end

  bt
end

# adds each key one at a time from the btree
#
# test_add(Btree, []) ->
#     {ok, Btree};
# test_add(Btree, [KeyValue | Rest]) ->
#     {ok, Btree2} = add_remove(Btree, [KeyValue], []),
#     test_add(Btree2, Rest).
def test_add(bt, key_values)
  key_values.each do |key_value|
    bt = BTree.add_remove(bt, [key_value], [])
  end
  bt
end

# Makes sure each key value pair is found in the btree
#
# test_lookup(_Btree, []) ->
#     ok;
# test_lookup(Btree, [{Key, Value} | Rest]) ->
#     [{ok,{Key, Value}}] = lookup(Btree, [Key]),
#     {ok, []} = foldl(Btree, Key, fun({KeyIn, ValueIn}, []) ->
#             KeyIn = Key,
#             ValueIn = Value,
#             {stop, []}
#         end,
#         []),
#     {ok, []} = foldr(Btree, Key, fun({KeyIn, ValueIn}, []) ->
#             KeyIn = Key,
#             ValueIn = Value,
#             {stop, []}
#         end,
#         []),
#     test_lookup(Btree, Rest).
def test_lookup(bt, key_values)
  key_values.each do |(key, value)|
    looked_up = BTree.lookup(bt, [key])

    p :lookup => looked_up
    fail("doesn't match") unless [[:ok, [key, value]]] == looked_up

    foldl_result = BTree.foldl(bt, key, []){|key_in, value_in|
      fail("key doesn't match") unless key_in == key
      fail("value doesn't match") unless value_in == value
      [:stop, []]
    }
    fail unless foldl_result == [:ok, []]

    foldr_result = BTree.foldr(bt, key, []){|key_in, value_in|
      fail("key doesn't match") unless key_in == key
      fail("value doesn't match") unless value_in == value
      [:stop, []]
    }
    fail unless foldr_result == [:ok, []]
  end
end

# every_other(List) ->
#     every_other(List, [], [], 1).
# 
# every_other([], AccA, AccB, _Flag) ->
#     {lists:reverse(AccA), lists:reverse(AccB)};
# every_other([H|T], AccA, AccB, 1) ->
#     every_other(T, [H|AccA], AccB, 0);
# every_other([H|T], AccA, AccB, 0) ->
#     every_other(T, AccA, [H|AccB], 1).
#
# This is a wonderful demonstration of the difference of OO and functional :)
def every_other(list)
  left, right = [], []

  list.each_with_index do |element, idx, *rest|
    (idx % 2 == 0) ? (left << element) : (right << element)
  end

  return left, right
end

def test_btree(key_values = [])
  require 'fileutils'
  FileUtils.rm('./foo')

  fd = CouchRB::Db::File.new('./foo', 'a+')
  btree = BTree.open(nil, fd)
  len = key_values.size

  reduce = lambda{|type, kvs|
    # p :reduce => {:type => type, :kvs => kvs}

    case type
    when :reduce
      kvs.size
    when :rereduce
      kvs.reduce(0){|s,(k,v)| k ? (s + k) : s }
    end
  }
  btree1 = BTree.set_options(btree, :reduce => reduce)

  puts '', 'first dump in all the values in one go', ''
  btree10 = BTree.add_remove(btree1, key_values, [])
  # p :btree10 => btree10

  # FIXME: this won't work :(((((
  # get the leading reduction as we foldl/r and count of all from start to val
  val = len / 3
  #
  #   require 'timeout'
  #   Timeout.timeout(3){
  #       BTree.foldl(btree10, val){|kvs, leading_reds, acc|
  #       p :acc => acc, :kvs => kvs, :leading_reds => leading_reds
  # #
  #       count_to_start = val + acc - 1
  #       final_reduced = BTree.final_reduce(btree10, leading_reds)
  # #
  #       fail("%p" % [{
  #         :count_to_start => count_to_start,
  #         :final_reduced => final_reduced
  #       }]) unless count_to_start == final_reduced
  # #
  #       [:ok, acc + 1]
  #     }
  #   }

  puts '', 'make sure all are in the tree', ''
  test_keys(btree10, key_values)

  puts '', 'remove everything', ''
  btree20 = test_remove(btree10, key_values)

  foldl_result = BTree.foldl(btree20, false){|_x, _acc|
    p :_x => _x, :_acc => _acc
    [:ok, true]
  }
  fail('foldl_result is not empty') unless foldl_result == [:ok, false]

  # add everything back one at a time
  btree30 = test_add(btree20, key_values)
  fail 'no btree30' unless btree30
  test_keys(btree30, key_values)

  key_values_rev = key_values.reverse

  # remove everything, in reverse order
  btree40 = test_remove(btree30, key_values_rev)
  fail 'no btree40' unless btree40

  # make sure it's empty
  foldl_result = BTree.foldl(btree40, false){|_x, _acc|
    [:ok, true]
  }
  fail("wrong foldl") unless foldl_result == [:ok, false]

  a, b = every_other(key_values)
  fail 'no a' unless a
  fail 'no b' unless b

  # add everything back
  btree50 = test_add(btree40, key_values)
  fail 'no btree50' unless btree50

  test_keys(btree50, key_values)

  # remove half the values
  btree60 = test_remove(btree50, a)
  fail 'no btree60' unless btree60

  # verify the remaining
  test_keys(btree60, b)

  # add a back
  btree70 = test_add(btree60, a)
  fail 'no btree70' unless btree70

  # verify
  test_keys(btree70, key_values)


#     % remove half the values
#     {ok, Btree60} = test_remove(Btree50, A),
# 
#     % verify the remaining
#     ok = test_keys(Btree60, B),
# 
#     % add A back
#     {ok, Btree70} = test_add(Btree60, A),
# 
#     % verify
#     ok = test_keys(Btree70, KeyValues),
# 
#     % remove B
#     {ok, Btree80} = test_remove(Btree70, B),
# 
#     % verify the remaining
#     ok = test_keys(Btree80, A),
#     
#     {ok, Btree90} = test_remove(Btree80, A),
#     
#     EvenOdd = fun(V) when V rem 2 == 1 -> "odd"; (_) -> "even" end,
#     
#     EvenOddKVs = [{{EvenOdd(Key),Key}, 1} || {Key, _} <- KeyValues],
# 
#     {ok, Btree100} = test_add(Btree90, EvenOddKVs),
#     GroupingFun = fun({K1, _},{K2,_}) -> K1 == K2 end,
#     FoldFun = fun(GroupedKey, Unreduced, Acc) ->
#             {ok, [{GroupedKey, final_reduce(Btree100, Unreduced)} | Acc]}
#         end,
#         
#     Half = Len div 2,
#     
#     {ok, [{{"odd", _}, Half}, {{"even",_}, Half}]} =
#         fold_reduce(Btree100, nil, nil, GroupingFun, FoldFun, []),
#     
#     {ok, [{{"even",_}, Half}, {{"odd", _}, Half}]} =
#         fold_reduce(Btree100, rev, nil, nil, GroupingFun, FoldFun, []),
#         
#     {ok, [{{"even",_}, Half}]} =
#         fold_reduce(Btree100, fwd, {"even", -1}, {"even", foo}, GroupingFun, FoldFun, []),
#         
#     {ok, [{{"even",_}, Half}]} =
#         fold_reduce(Btree100, rev, {"even", foo}, {"even", -1}, GroupingFun, FoldFun, []),
#         
#     {ok, [{{"odd",_}, Half}]} =
#         fold_reduce(Btree100, fwd, {"odd", -1}, {"odd", foo}, GroupingFun, FoldFun, []),
#     
#     {ok, [{{"odd",_}, Half}]} =
#         fold_reduce(Btree100, rev, {"odd", foo}, {"odd", -1}, GroupingFun, FoldFun, []),
#     
#     {ok, [{{"odd", _}, Half}, {{"even",_}, Half}]} =
#         fold_reduce(Btree100, {"even", -1}, {"odd", foo}, GroupingFun, FoldFun, []),
#     
#     ok = couch_file:close(Fd).
end

n = 10

list = Array.new(n){|i| i }
test_btree(list)
test_btree(list.reverse)
test_btree(list.sort_by{ rand })
