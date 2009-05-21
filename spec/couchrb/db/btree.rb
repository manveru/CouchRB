require 'spec/helper'
require 'couchrb/db/btree'
require 'couchrb/db/file'

shared :test_helpers do
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
    fold_fun = lambda{|element, (hacc, *tacc)|
      element.should == hacc
      [:ok, tacc]
    }

    sorted = list.sort
    BTree.foldl(bt, sorted, &fold_fun).should == [:ok, []]

    BTree.foldr(bt, sorted.reverse, &fold_fun).should == [:ok, []]
  end

  # removes each key one at a time from the btree
  #
  # test_remove(Btree, []) ->
  #     {ok, Btree};
  # test_remove(Btree, [{Key, _Value} | Rest]) ->
  #     {ok, Btree2} = add_remove(Btree,[], [Key]),
  #     test_remove(Btree2, Rest).
  def test_remove(bt, kvs)
    kvs.each do |key, value|
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
    key_values.each do |key, value|
      looked_up = BTree.lookup(bt, [key])
      looked_up.should == [[:ok, [key, value]]]

      foldl_result = BTree.foldl(bt, key, []){|key_in, value_in|
        key_in.should == key
        value_in.should == value
        [:stop, []]
      }
      foldl_result.should == [:ok, []]

      foldr_result = BTree.foldr(bt, key, []){|key_in, value_in|
        key_in.should == key
        value_in.should == value
        [:stop, []]
      }
      foldr_result.should == [:ok, []]
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
end

BTree = CouchRB::Db::BTree
Terms = CouchRB::Db::Terms

describe CouchRB::Db::BTree do
  should 'have op_order' do
    BTree.op_order(:fetch).should == 1
    BTree.op_order(:remove).should == 2
    BTree.op_order(:insert).should == 3
  end

  should 'have adjust_dir' do
    BTree.adjust_dir(:fwd, [1,2,3]).should == [1,2,3]
    BTree.adjust_dir(:rev, [1,2,3]).should == [3,2,1]
  end

  should 'have find_first_gteq' do
    tuple = (50..100).to_a
    from = 0
    to = tuple.size
    key = 75

    BTree.find_first_gteq(tuple, from, to, key).
      should == 26
  end

  describe 'original tests' do
    behaves_like :test_helpers
    @key_values = Terms::List[]
    3.times{|n| @key_values << Terms::Tuple[n + 1, rand] }

    should 'create a new BTree' do
      FileUtils.rm('./foo')
      fd = CouchRB::Db::File.new('./foo', 'a+')
      @btree = BTree.open(nil, fd)
      @btree.fd.should == fd
    end

    should 'set a reduce function' do
      reduce = lambda{|type, kvs|
        case type
        when :reduce
          kvs.size
        when :rereduce
          kvs.reduce(0){|s,(k,v)| k ? (s + k) : s }
        end
      }

      @btree1 = BTree.set_options(@btree, :reduce => reduce)
      @btree1.reduce.should == reduce
    end

    should 'dump all key_values in one go and make sure they stay there' do
      puts
      @btree10 = BTree.add_remove(@btree1, @key_values.dup, [])
      @btree10.root.should.not == @btree1.root
    end

    should 'get the leading reduction as we foldl' do
      len = @key_values.size
      val = len / 3

      foldl_result = BTree.foldl(@btree10, 0, val){|_x, leading_reds, acc|
        # p :_x => _x, :leading_reds => leading_reds, :acc => acc
        count_to_start = val + acc - 1
        count_to_start.should == BTree.final_reduce(@btree10, leading_reds)
        [:ok, acc + 1]
      }

      foldl_result.first.should == :ok
    end
  end
end
__END__

    should 'get leading reduction as we foldr' do
      len = @key_values.size
      val = len / 3

      foldr_result = BTree.foldr(@btree10, 0, val){|_x, leading_reds, acc|
        count_to_end = len - val
        count_to_end.should == BTree.final_reduce(@btree10, leading_reds)
        [:stop, true] # change acc to true
      }

      foldr_result.should == [:ok, true]
    end

    should 'remove everything' do
      puts
      @btree20 = test_remove(@btree10, @key_values.dup)

      foldl_result = BTree.foldl(@btree20, false){|_x, _acc|
        p :_x => _x, :_acc => _acc
        [:ok, true]
      }
      foldl_result.should == [:ok, false]
    end
  end
end
__END__

    should 'remove everything' do
      bt12 = test_remove(@bt10, @key_values.dup)

      foldl_result = BTree.foldl(bt12, false){|_x, _acc|
        # p :_x => _x, :_acc => _acc
        [:ok, true]
      }
      foldl_result.should == [:ok, false]
    end
  end
end

__END__

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

list = Array.new(n){|i| [i, rand] }
test_btree(list)
test_btree(list.reverse)
test_btree(list.sort_by{ rand })
  end
end

__END__

#original CouchDB btree tests

BTree = CouchRB::Db::BTree

require 'lib/couchrb/db/file'

