from valkey_bloom_test_case import ValkeyBloomTestCaseBase
from valkeytests.conftest import resource_port_tracker

class TestBloomCommand(ValkeyBloomTestCaseBase):

    def verify_command_arity(self, command, expected_arity): 
        command_info = self.client.execute_command('COMMAND', 'INFO', command)
        actual_arity = command_info.get(command).get('arity')
        assert actual_arity == expected_arity, f"Arity mismatch for command '{command}'"

    def test_bloom_command_arity(self):
        self.verify_command_arity('BF.EXISTS', -1)
        self.verify_command_arity('BF.ADD', -1)
        self.verify_command_arity('BF.MEXISTS', -1)
        self.verify_command_arity('BF.MADD', -1)
        self.verify_command_arity('BF.CARD', -1)
        self.verify_command_arity('BF.RESERVE', -1)
        self.verify_command_arity('BF.INFO', -1)
        self.verify_command_arity('BF.INSERT', -1)

    def test_bloom_command_error(self):
        # test set up
        assert self.client.execute_command('BF.ADD key item') == 1
        assert self.client.execute_command('BF.RESERVE bf 0.01 1000') == b'OK'

        basic_error_test_cases = [
            # not found
            ('BF.INFO TEST404', 'not found'),
            # incorrect syntax and argument usage
            ('bf.info key item', 'invalid information value'),
            ('bf.insert key CAPACITY 10000 ERROR 0.01 EXPANSION 0.99 NOCREATE NONSCALING ITEMS test1 test2 test3', 'bad expansion'),
            ('BF.INSERT KEY HELLO WORLD', 'unknown argument received'),
            ('BF.INSERT KEY error 2 ITEMS test1', '(0 < error rate range < 1)'),
            ('BF.INSERT KEY ERROR err ITEMS test1', 'bad error rate'),
            ('BF.INSERT KEY TIGHTENING tr ITEMS test1', 'bad tightening ratio'),
            ('BF.INSERT KEY TIGHTENING 2 ITEMS test1', '(0 < tightening ratio range < 1)'),
            ('BF.INSERT TEST_LIMIT ERROR 0.99999999999999999 ITEMS ERROR_RATE', '(0 < error rate range < 1)'),
            ('BF.INSERT TEST_LIMIT TIGHTENING 0.99999999999999999 ITEMS ERROR_RATE', '(0 < tightening ratio range < 1)'),
            ('BF.INSERT TEST_LIMIT CAPACITY 9223372036854775808 ITEMS CAP', 'bad capacity'),
            ('BF.INSERT TEST_LIMIT CAPACITY 0 ITEMS CAP0', '(capacity should be larger than 0)'),
            ('BF.INSERT TEST_LIMIT EXPANSION 4294967299 ITEMS EXPAN', 'bad expansion'),
            ('BF.INSERT TEST_NOCREATE NOCREATE ITEMS A B', 'not found'),
            ('BF.INSERT KEY HELLO', 'unknown argument received'),
            ('BF.INSERT KEY CAPACITY 1 ERROR 0.0000000001 VALIDATESCALETO 10000000 EXPANSION 1', 'provided VALIDATESCALETO causes false positive to degrade to 0'),
            ('BF.INSERT KEY VALIDATESCALETO 1000000000000', 'provided VALIDATESCALETO causes bloom object to exceed memory limit'),
            ('BF.INSERT KEY VALIDATESCALETO 1000000000000 NONSCALING', 'cannot use NONSCALING and VALIDATESCALETO options together'),
            ('BF.RESERVE KEY String 100', 'bad error rate'),
            ('BF.RESERVE KEY 0.99999999999999999 3000', '(0 < error rate range < 1)'),
            ('BF.RESERVE KEY 2 100', '(0 < error rate range < 1)'),
            ('BF.RESERVE KEY 0.01 String', 'bad capacity'),
            ('BF.RESERVE KEY 0.01 0.01', 'bad capacity'),
            ('BF.RESERVE KEY 0.01 -1', 'bad capacity'),
            ('BF.RESERVE KEY 0.01 9223372036854775808', 'bad capacity'),
            ('BF.RESERVE bf 0.01 1000', 'item exists'),
            ('BF.RESERVE TEST_CAP 0.50 0', '(capacity should be larger than 0)'),

            # wrong number of arguments
            ('BF.ADD TEST', 'wrong number of arguments for \'BF.ADD\' command'),
            ('BF.ADD', 'wrong number of arguments for \'BF.ADD\' command'),
            ('BF.ADD HELLO TEST WORLD', 'wrong number of arguments for \'BF.ADD\' command'),
            ('BF.CARD KEY ITEM', 'wrong number of arguments for \'BF.CARD\' command'),
            ('bf.card', 'wrong number of arguments for \'BF.CARD\' command'),
            ('BF.EXISTS', 'wrong number of arguments for \'BF.EXISTS\' command'),
            ('bf.exists item', 'wrong number of arguments for \'BF.EXISTS\' command'),
            ('bf.exists key item hello', 'wrong number of arguments for \'BF.EXISTS\' command'),
            ('BF.INFO', 'wrong number of arguments for \'BF.INFO\' command'),
            ('bf.info key capacity size', 'wrong number of arguments for \'BF.INFO\' command'),
            ('BF.INSERT', 'wrong number of arguments for \'BF.INSERT\' command'),
            ('BF.INSERT MISS_ITEM EXPANSION 2 ITEMS', 'wrong number of arguments for \'BF.INSERT\' command'),
            ('BF.INSERT MISS_VAL ERROR 0.5 EXPANSION', 'wrong number of arguments for \'BF.INSERT\' command'),
            ('BF.INSERT MISS_VAL ERROR 0.5 CAPACITY', 'wrong number of arguments for \'BF.INSERT\' command'),
            ('BF.INSERT MISS_VAL EXPANSION 2 EXPANSION', 'wrong number of arguments for \'BF.INSERT\' command'),
            ('BF.INSERT MISS_VAL EXPANSION 1 error', 'wrong number of arguments for \'BF.INSERT\' command'),
            ('BF.MADD', 'wrong number of arguments for \'BF.MADD\' command'),
            ('BF.MADD KEY', 'wrong number of arguments for \'BF.MADD\' command'),
            ('BF.MEXISTS', 'wrong number of arguments for \'BF.MEXISTS\' command'),
            ('BF.MEXISTS INFO', 'wrong number of arguments for \'BF.MEXISTS\' command'),
            ('BF.RESERVE', 'wrong number of arguments for \'BF.RESERVE\' command'),
            ('BF.RESERVE KEY', 'wrong number of arguments for \'BF.RESERVE\' command'),
            ('BF.RESERVE KEY SSS', 'wrong number of arguments for \'BF.RESERVE\' command'),
            ('BF.RESERVE TT1 0.01 1 NONSCALING test1 test2 test3', 'wrong number of arguments for \'BF.RESERVE\' command'),
            ('BF.RESERVE TT 0.01 1 NONSCALING EXPANSION 1', 'wrong number of arguments for \'BF.RESERVE\' command'),
        ]

        for test_case in basic_error_test_cases:
            cmd = test_case[0]
            expected_err_reply = test_case[1]
            self.verify_error_response(self.client, cmd, expected_err_reply)

    def test_bloom_command_behavior(self):
        basic_behavior_test_case = [
            ('BF.ADD key item', 1),
            ('BF.ADD key item', 0),
            ('BF.EXISTS key item', 1),
            ('BF.MADD key item item2', 2),
            ('BF.EXISTS key item', 1),
            ('BF.EXISTS key item2', 1),
            ('BF.MADD hello world1 world2 world3', 3),
            ('BF.MADD hello world1 world2 world3 world4', 4),
            ('BF.MEXISTS hello world5', 1),
            ('BF.MADD hello world5', 1),
            ('BF.MEXISTS hello world5 world6 world7', 3),
            ('BF.INSERT TEST ITEMS ITEM', 1),
            ('BF.INSERT TEST CAPACITY 1000 ITEMS ITEM', 1),
            ('BF.INSERT TEST CAPACITY 200 error 0.50 ITEMS ITEM ITEM1 ITEM2', 3),
            ('BF.INSERT TEST CAPACITY 300 ERROR 0.50 EXPANSION 1 ITEMS ITEM FOO', 2),
            ('BF.INSERT TEST ERROR 0.50 EXPANSION 3 NOCREATE items BOO', 1), 
            ('BF.INSERT TEST ERROR 0.50 EXPANSION 1 NOCREATE NONSCALING items BOO', 1),
            ('BF.INSERT TEST_EXPANSION EXPANSION 9 ITEMS ITEM', 1),
            ('BF.INSERT TEST_CAPACITY CAPACITY 2000 ITEMS ITEM', 1),
            ('BF.INSERT TEST_ITEMS ITEMS 1 2 3 EXPANSION 2', 5),
            ('BF.INSERT TEST_VAL_SCALE_1 CAPACITY 200 VALIDATESCALETO 1000000 error 0.0001 ITEMS ITEM ITEM1 ITEM2', 3),
            ('BF.INSERT TEST_VAL_SCALE_2 CAPACITY 20000 VALIDATESCALETO 10000000 error 0.5 EXPANSION 4 ITEMS ITEM ITEM1 ITEM2', 3),
            ('BF.INSERT TEST_VAL_SCALE_3 CAPACITY 10400 VALIDATESCALETO 10410 error 0.0011 EXPANSION 1 ITEMS ITEM ITEM1 ITEM2', 3),
            ('BF.INSERT KEY', 0),
            ('BF.INSERT KEY EXPANSION 2', 0),
            ('BF.INFO TEST Capacity', 100),
            ('BF.INFO TEST ITEMS', 5),
            ('BF.INFO TEST filters', 1),
            ('bf.info TEST expansion', 2),
            ('BF.INFO TEST_EXPANSION EXPANSION', 9),
            ('BF.INFO TEST_CAPACITY CAPACITY', 2000),
            ('BF.INFO TEST MAXSCALEDCAPACITY', 26214300),
            ('BF.INFO TEST_VAL_SCALE_1 ERROR', b'0.0001'),
            ('BF.INFO TEST_VAL_SCALE_2 ERROR', b'0.5'),
            ('BF.CARD key', 3),
            ('BF.CARD hello', 5),
            ('BF.CARD TEST', 5),
            ('bf.card HELLO', 0),
            ('BF.RESERVE bf 0.01 1000', b'OK'),
            ('BF.EXISTS bf non_existant', 0),
            ('BF.RESERVE bf_exp 0.01 1000 EXPANSION 2', b'OK'),
            ('BF.RESERVE bf_non 0.01 1000 NONSCALING', b'OK'),
            ('bf.info bf_exp expansion', 2),
            ('BF.INFO bf_non expansion', None),
        ]

        for test_case in basic_behavior_test_case:
            cmd = test_case[0]
            # For non multi commands, this is the verbatim expected result. 
            # For multi commands, test_case[1] contains the number of item add/exists results which are expected to be 0 or 1.
            expected_result = test_case[1]
            # For Cardinality commands we want to add items till we are at the number of items we expect then check Cardinality worked
            if cmd.upper().startswith("BF.CARD"):
                self.add_items_till_capacity(self.client, cmd.split()[-1], expected_result, 1, "item_prefix")
            # For multi commands expected result is actually the length of the expected return. While for other commands this we have the literal
            # expected result
            self.verify_command_success_reply(self.client, cmd, expected_result)

        # test bf.info
        assert self.client.execute_command('BF.RESERVE BF_INFO 0.50 2000 NONSCALING') == b'OK'
        bf_info = self.client.execute_command('BF.INFO BF_INFO')
        capacity_index = bf_info.index(b'Capacity') + 1
        filter_index = bf_info.index(b'Number of filters') + 1
        item_index = bf_info.index(b'Number of items inserted') + 1
        expansion_index = bf_info.index(b'Expansion rate') + 1
        assert bf_info[capacity_index] == self.client.execute_command('BF.INFO BF_INFO CAPACITY') == 2000
        assert bf_info[filter_index] == self.client.execute_command('BF.INFO BF_INFO FILTERS') == 1
        assert bf_info[item_index] == self.client.execute_command('BF.INFO BF_INFO ITEMS') == 0
        assert bf_info[expansion_index] == self.client.execute_command('BF.INFO BF_INFO EXPANSION') == None
