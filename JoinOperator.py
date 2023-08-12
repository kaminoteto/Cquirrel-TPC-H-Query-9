from typing import Tuple
from pyflink.common.state import ListState, ListStateDescriptor, ValueState, ValueStateDescriptor
from pyflink.common import Date
from pyflink.common.functions import CoProcessFunction
from pyflink.util import Collector


class ConnectPartAndLine(CoProcessFunction[Tuple, Tuple, Tuple]):
    def open(self, parameters):
        partState = self._runtime_context.get_state(ValueStateDescriptor("partState", Part))
        lineListState = self._runtime_context.get_state(ListStateDescriptor("lineState", Line))

    def processElement1(self, part, context: 'CoProcessFunction.Context', collector: 'Collector[Tuple]'):
        oldPart = partState.value()

        if ((oldPart is None and part.update) or (oldPart is not None and not part.update)):
            partState.update(part)
            lines = lineListState.get()
            for line in lines:
                update = OutputBoolean.is_update(part.update, line.update)
                tuple = (update, part.name, part.partKey, line.lineKey, line.lineDate, line.totalQuantity)
                if OutputBoolean.can_collect(part.update, line.update):
                    collector.collect(tuple)

    def processElement2(self, line, context: 'CoProcessFunction.Context', collector: 'Collector[Tuple]'):
        can_add_to_list = line.update
        if not can_add_to_list:
            insert_count = 0
            delete_count = 0
            lines = lineListState.get()
            for l in lines:
                if l.update:
                    insert_count += 1
                else:
                    delete_count += 1
            if insert_count > delete_count:
                can_add_to_list = True

        if not can_add_to_list:
            return

        lineListState.add(line)
        part = partState.value()
        if part:
            update = OutputBoolean.is_update(part.update, line.update)
            tuple = (update, part.name, part.partKey, line.lineKey, line.lineDate, line.totalQuantity)
            if OutputBoolean.can_collect(part.update, line.update):
                collector.collect(tuple)


class ConnectTmpAndPartsupp(CoProcessFunction[Tuple, Tuple, Tuple]):
    def open(self, parameters):
        tmpListState = self._runtime_context.get_state(ListStateDescriptor("tmpList", Part))
        partsuppListState = self._runtime_context.get_state(ListStateDescriptor("partsuppList", Partsupp))

    def processElement1(self, tmp, context: 'CoProcessFunction.Context', collector: 'Collector[Tuple]'):
        tmpListState.add(tmp)
        for partsupp in partsuppListState.get():
            if tmp.partKey == partsupp.partKey:
                tuple = (tmp.orderKey, tmp.quantity, tmp.discount, partsupp.availQty, partsupp.supplyCost)
                collector.collect(tuple)

    def processElement2(self, partsupp, context: 'CoProcessFunction.Context', collector: 'Collector[Tuple]'):
        partsuppListState.add(partsupp)
        for tmp in tmpListState.get():
            if tmp.partKey == partsupp.partKey:
                tuple = (tmp.orderKey, tmp.quantity, tmp.discount, partsupp.availQty, partsupp.supplyCost)
                collector.collect(tuple)


class ConnectTmpAndOrders(CoProcessFunction[Tuple, Tuple, Tuple]):
    def open(self, parameters):
        tmpListState = self._runtime_context.get_state(ListStateDescriptor("tmpList", Temp))
        ordersListState = self._runtime_context.get_state(ListStateDescriptor("ordersList", Orders))

    def processElement1(self, tmp, context: 'CoProcessFunction.Context', collector: 'Collector[Tuple]'):
        tmpListState.add(tmp)
        for orders in ordersListState.get():
            if tmp.orderKey == orders.orderKey:
                tuple = (tmp.partKey, tmp.quantity, tmp.discount, orders.orderDate, orders.totalPrice)
                collector.collect(tuple)

    def processElement2(self, orders, context: 'CoProcessFunction.Context', collector: 'Collector[Tuple]'):
        ordersListState.add(orders)
        for tmp in tmpListState.get():
            if tmp.orderKey == orders.orderKey:
                tuple = (tmp.partKey, tmp.quantity, tmp.discount, orders.orderDate, orders.totalPrice)
                collector.collect(tuple)


class ConnectTmpAndSupplier(CoProcessFunction[Tuple, Tuple, Tuple]):
    def open(self, parameters):
        tmpListState = self._runtime_context.get_state(ListStateDescriptor("tmpList", TempData))
        supplierListState = self._runtime_context.get_state(ListStateDescriptor("supplierList", Supplier))

    def processElement1(self, tmp, context: 'CoProcessFunction.Context', collector: 'Collector[Tuple]'):
        tmpListState.add(tmp)
        for supplier in supplierListState.get():
            if tmp.supplierKey == supplier.supplierKey:
                tuple = (tmp.orderKey, tmp.partKey, tmp.quantity, supplier.nationKey, supplier.regionKey)
                collector.collect(tuple)

    def processElement2(self, supplier, context: 'CoProcessFunction.Context', collector: 'Collector[Tuple]'):
        supplierListState.add(supplier)
        for tmp in tmpListState.get():
            if tmp.supplierKey == supplier.supplierKey:
                tuple = (tmp.orderKey, tmp.partKey, tmp.quantity, supplier.nationKey, supplier.regionKey)
                collector.collect(tuple)


class ConnectTmpAndNation(CoProcessFunction[Tuple, Tuple, Tuple]):
    def open(self, parameters):
        tmpListState = self._runtime_context.get_state(ListStateDescriptor("tmpList", TmpData))
        nationListState = self._runtime_context.get_state(ListStateDescriptor("nationList", Nation))

    def processElement1(self, tmp, context: 'CoProcessFunction.Context', collector: 'Collector[Tuple]'):
        tmpListState.add(tmp)
        for nation in nationListState.get():
            if tmp.nationKey == nation.nationKey:
                tuple = (tmp.orderKey, tmp.partKey, tmp.quantity, tmp.discount, nation.regionKey)
                collector.collect(tuple)

    def processElement2(self, nation, context: 'CoProcessFunction.Context', collector: 'Collector[Tuple]'):
        nationListState.add(nation)
        for tmp in tmpListState.get():
            if tmp.nationKey == nation.nationKey:
                tuple = (tmp.orderKey, tmp.partKey, tmp.quantity, tmp.discount, nation.regionKey)
                collector.collect(tuple)