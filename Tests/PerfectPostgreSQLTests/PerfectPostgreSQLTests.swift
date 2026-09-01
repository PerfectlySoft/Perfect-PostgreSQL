import Foundation
import Testing
import PerfectCRUD
@testable import PerfectPostgreSQL

let testDBRowCount = 5
let postgresTestDBName = "testing123"
let postgresInitConnInfo = "host=localhost dbname=postgres"
let postgresTestConnInfo = "host=localhost dbname=testing123"
typealias DBConfiguration = PostgresDatabaseConfiguration

func getDB(reset: Bool = true) throws -> Database<DBConfiguration> {
    if reset {
        let db = Database(configuration: try DBConfiguration(postgresInitConnInfo))
        try? db.sql("DROP DATABASE \(postgresTestDBName)")
        try db.sql("CREATE DATABASE \(postgresTestDBName)")
    }
    return Database(configuration: try DBConfiguration(postgresTestConnInfo))
}

@Suite struct PerfectPostgreSQLTests {

    struct TestTable1: Codable, TableNameProvider {
        enum CodingKeys: String, CodingKey {
            case id, name, integer = "int", double = "doub", blob, subTables
        }
        static let tableName = "test_table_1"
        @PrimaryKey var id: Int
        let name: String?
        let integer: Int?
        let double: Double?
        let blob: [UInt8]?
        let subTables: [TestTable2]?
        init(id: Int, name: String? = nil, integer: Int? = nil,
             double: Double? = nil, blob: [UInt8]? = nil, subTables: [TestTable2]? = nil) {
            self.id = id
            self.name = name
            self.integer = integer
            self.double = double
            self.blob = blob
            self.subTables = subTables
        }
    }

    struct TestTable2: Codable {
        @PrimaryKey var id: UUID
        @ForeignKey(TestTable1.self, onDelete: cascade, onUpdate: cascade) var parentId: Int
        let date: Date
        let name: String?
        let int: Int?
        let doub: Double?
        let blob: [UInt8]?
        init(id: UUID, parentId: Int, date: Date, name: String? = nil,
             int: Int? = nil, doub: Double? = nil, blob: [UInt8]? = nil) {
            self.id = id
            self.date = date
            self.name = name
            self.int = int
            self.doub = doub
            self.blob = blob
            self.parentId = parentId
        }
    }

    let pgEnabled = ProcessInfo.processInfo.environment["PG_TESTS"] == "1"

    init() {
        CRUDClearTableStructureCache()
    }

    func getTestDB() throws -> Database<DBConfiguration> {
        let db = try getDB()
        try db.create(TestTable1.self, policy: .dropTable)
        try db.transaction { () -> Void in
            _ = try db.table(TestTable1.self)
                .insert((1...testDBRowCount).map { num -> TestTable1 in
                    let n = UInt8(num)
                    let blob: [UInt8]? = (num % 2 != 0) ? nil : [UInt8](arrayLiteral: n+1, n+2, n+3, n+4, n+5)
                    return TestTable1(id: num, name: "This is name bind \(num)",
                                     integer: num, double: Double(num), blob: blob)
                })
        }
        try db.transaction { () -> Void in
            _ = try db.table(TestTable2.self)
                .insert((1...testDBRowCount).flatMap { parentId -> [TestTable2] in
                    (1...testDBRowCount).map { num -> TestTable2 in
                        let n = UInt8(num)
                        let blob: [UInt8]? = [UInt8](arrayLiteral: n+1, n+2, n+3, n+4, n+5)
                        return TestTable2(id: UUID(), parentId: parentId, date: Date(),
                                         name: num % 2 == 0 ? "This is name bind \(num)" : "me",
                                         int: num, doub: Double(num), blob: blob)
                    }
                })
        }
        return try getDB(reset: false)
    }

    @Test func create1() throws {
        guard pgEnabled else { return }
        let db = try getDB()
        try db.create(TestTable1.self, policy: .dropTable)
        do {
            let t2 = db.table(TestTable2.self)
            try t2.index(\.parentId)
        }
        let t1 = db.table(TestTable1.self)
        let t2 = db.table(TestTable2.self)
        let subId = UUID()
        try db.transaction {
            let newOne = TestTable1(id: 2000, name: "New One", integer: 40)
            try t1.insert(newOne)
            let newSub1 = TestTable2(id: subId, parentId: 2000, date: Date(), name: "Me")
            let newSub2 = TestTable2(id: UUID(), parentId: 2000, date: Date(), name: "Not Me")
            try t2.insert([newSub1, newSub2])
        }
        let j21 = try t1.join(\.subTables, on: \.id, equals: \.parentId)
        let j2 = j21.where(\TestTable1.id == 2000 && \TestTable2.name == "Me")
        let j3 = j21.where(\TestTable1.id > 20 &&
            !(\TestTable1.name == "Me" || \TestTable1.name == "You"))
        #expect(try j3.count() == 1)
        try db.transaction {
            let j2a = try j2.select().map { $0 }
            #expect(try j2.count() == 1)
            #expect(j2a.count == 1)
            guard j2a.count == 1 else { return }
            let obj = j2a[0]
            #expect(obj.id == 2000)
            #expect(obj.subTables != nil)
            let subTables = obj.subTables!
            #expect(subTables.count == 1)
            let obj2 = subTables[0]
            #expect(obj2.id == subId)
        }
        try db.create(TestTable1.self)
        do {
            let j2a = try j2.select().map { $0 }
            #expect(try j2.count() == 1)
            #expect(j2a[0].id == 2000)
        }
        try db.create(TestTable1.self, policy: .dropTable)
        do {
            let j2b = try j2.select().map { $0 }
            #expect(j2b.count == 0)
        }
    }

    @Test func create2() throws {
        guard pgEnabled else { return }
        let db = try getTestDB()
        try db.create(TestTable1.self, primaryKey: \.id, policy: .dropTable)
        do {
            let t2 = db.table(TestTable2.self)
            try t2.index(\.parentId, \.date)
        }
        let t1 = db.table(TestTable1.self)
        do {
            let newOne = TestTable1(id: 2000, name: "New One", integer: 40)
            try t1.insert(newOne)
        }
        let j2 = try t1.where(\TestTable1.id == 2000).select()
        do {
            let j2a = j2.map { $0 }
            #expect(j2a.count == 1)
            #expect(j2a[0].id == 2000)
        }
        try db.create(TestTable1.self)
        do {
            let j2a = j2.map { $0 }
            #expect(j2a.count == 1)
            #expect(j2a[0].id == 2000)
        }
        try db.create(TestTable1.self, policy: .dropTable)
        do {
            let j2b = j2.map { $0 }
            #expect(j2b.count == 0)
        }
    }

    @Test func create3() throws {
        guard pgEnabled else { return }
        struct FakeTestTable1: Codable, TableNameProvider {
            enum CodingKeys: String, CodingKey {
                case id, name, double = "doub", double2 = "doub2", blob, subTables
            }
            static let tableName = "test_table_1"
            let id: Int
            let name: String?
            let double2: Double?
            let double: Double?
            let blob: [UInt8]?
            let subTables: [TestTable2]?
        }
        let db = try getTestDB()
        try db.create(TestTable1.self, policy: [.dropTable, .shallow])
        do {
            let t1 = db.table(TestTable1.self)
            let newOne = TestTable1(id: 2000, name: "New One", integer: 40)
            try t1.insert(newOne)
        }
        try db.create(FakeTestTable1.self, policy: [.reconcileTable, .shallow])
        let t1 = db.table(FakeTestTable1.self)
        let j2 = try t1.where(\FakeTestTable1.id == 2000).select()
        let j2a = j2.map { $0 }
        #expect(j2a.count == 1)
        #expect(j2a[0].id == 2000)
    }

    @Test func selectAll() throws {
        guard pgEnabled else { return }
        let db = try getTestDB()
        let j2 = db.table(TestTable1.self)
        for row in try j2.select() {
            #expect(row.subTables == nil)
        }
    }

    @Test func selectIn() throws {
        guard pgEnabled else { return }
        let db = try getTestDB()
        let table = db.table(TestTable1.self)
        #expect(try table.where(\TestTable1.id ~ [2, 4]).count() == 2)
        #expect(try table.where(\TestTable1.id !~ [2, 4]).count() == 3)
    }

    @Test func selectLikeString() throws {
        guard pgEnabled else { return }
        let db = try getTestDB()
        let table = db.table(TestTable2.self)
        #expect(try table.where(\TestTable2.name %=% "me").count() == 25)
        #expect(try table.where(\TestTable2.name =% "me").count() == 15)
        #expect(try table.where(\TestTable2.name %= "me").count() == 15)
        #expect(try table.where(\TestTable2.name %!=% "me").count() == 0)
        #expect(try table.where(\TestTable2.name !=% "me").count() == 10)
        #expect(try table.where(\TestTable2.name %!= "me").count() == 10)
    }

    @Test func selectJoin() throws {
        guard pgEnabled else { return }
        let db = try getTestDB()
        let j2 = try db.table(TestTable1.self)
            .order(by: \TestTable1.name)
            .join(\.subTables, on: \.id, equals: \.parentId)
            .order(by: \.id)
            .where(\TestTable2.name == "me")
        let j2c = try j2.count()
        let j2a = try j2.select().map { $0 }
        let j2ac = j2a.count
        #expect(j2c != 0)
        #expect(j2c == j2ac)
        j2a.forEach { row in
            #expect(!(row.subTables?.isEmpty ?? true))
        }
    }

    @Test func insert1() throws {
        guard pgEnabled else { return }
        let db = try getTestDB()
        let t1 = db.table(TestTable1.self)
        let newOne = TestTable1(id: 2000, name: "New One", integer: 40)
        try t1.insert(newOne)
        let j1 = t1.where(\TestTable1.id == newOne.id)
        let j2 = try j1.select().map { $0 }
        #expect(try j1.count() == 1)
        #expect(j2[0].id == 2000)
    }

    @Test func insert2() throws {
        guard pgEnabled else { return }
        let db = try getTestDB()
        let t1 = db.table(TestTable1.self)
        let newOne = TestTable1(id: 2000, name: "New One", integer: 40)
        try t1.insert(newOne, ignoreKeys: \TestTable1.integer)
        let j1 = t1.where(\TestTable1.id == newOne.id)
        let j2 = try j1.select().map { $0 }
        #expect(try j1.count() == 1)
        #expect(j2[0].id == 2000)
        #expect(j2[0].integer == nil)
    }

    @Test func insert3() throws {
        guard pgEnabled else { return }
        let db = try getTestDB()
        let t1 = db.table(TestTable1.self)
        let newOne = TestTable1(id: 2000, name: "New One", integer: 40)
        let newTwo = TestTable1(id: 2001, name: "New One", integer: 40)
        try t1.insert([newOne, newTwo], setKeys: \TestTable1.id, \TestTable1.integer)
        let j1 = t1.where(\TestTable1.id == newOne.id)
        let j2 = try j1.select().map { $0 }
        #expect(try j1.count() == 1)
        #expect(j2[0].id == 2000)
        #expect(j2[0].integer == 40)
        #expect(j2[0].name == nil)
    }

    @Test func update() throws {
        guard pgEnabled else { return }
        let db = try getTestDB()
        let newOne = TestTable1(id: 2000, name: "New One", integer: 40)
        let newId: Int = try db.transaction {
            try db.table(TestTable1.self).insert(newOne)
            let newOne2 = TestTable1(id: 2000, name: "New👻One Updated", integer: 41)
            try db.table(TestTable1.self)
                .where(\TestTable1.id == newOne.id)
                .update(newOne2, setKeys: \.name)
            return newOne2.id
        }
        let j2 = try db.table(TestTable1.self)
            .where(\TestTable1.id == newId)
            .select().map { $0 }
        #expect(j2.count == 1)
        #expect(j2[0].id == 2000)
        #expect(j2[0].name == "New👻One Updated")
        #expect(j2[0].integer == 40)
    }

    @Test func delete() throws {
        guard pgEnabled else { return }
        let db = try getTestDB()
        let t1 = db.table(TestTable1.self)
        let newOne = TestTable1(id: 2000, name: "New One", integer: 40)
        try t1.insert(newOne)
        let query = t1.where(\TestTable1.id == newOne.id)
        let j1 = try query.select().map { $0 }
        #expect(j1.count == 1)
        try query.delete()
        let j2 = try query.select().map { $0 }
        #expect(j2.count == 0)
    }

    @Test func selectLimit() throws {
        guard pgEnabled else { return }
        let db = try getTestDB()
        #expect(try db.table(TestTable1.self).limit(2, skip: 2).select().map { $0 }.count == 2)
        #expect(try db.table(TestTable1.self).limit(2...3).select().map { $0 }.count == 2)
        #expect(try db.table(TestTable1.self).limit(2..<4).select().map { $0 }.count == 2)
        #expect(try db.table(TestTable1.self).limit(...1).select().map { $0 }.count == 2)
        #expect(try db.table(TestTable1.self).limit(..<2).select().map { $0 }.count == 2)
        #expect(try db.table(TestTable1.self).limit(3...).select().map { $0 }.count == 2)
        #expect(try db.table(TestTable1.self).limit(2, skip: 2).count() == 2)
        #expect(try db.table(TestTable1.self).limit(2...3).count() == 2)
        #expect(try db.table(TestTable1.self).limit(2..<4).count() == 2)
        #expect(try db.table(TestTable1.self).limit(...1).count() == 2)
        #expect(try db.table(TestTable1.self).limit(..<2).count() == 2)
        #expect(try db.table(TestTable1.self).limit(3...).count() == 2)
    }

    @Test func selectLimitWhere() throws {
        guard pgEnabled else { return }
        let db = try getTestDB()
        let j2 = db.table(TestTable1.self).limit(3).where(\TestTable1.id > 3)
        #expect(try j2.count() == 2)
        #expect(try j2.select().map { $0 }.count == 2)
    }

    @Test func selectOrderLimitWhere() throws {
        guard pgEnabled else { return }
        let db = try getTestDB()
        let j2 = db.table(TestTable1.self).order(by: \TestTable1.id).limit(3).where(\TestTable1.id > 3)
        #expect(try j2.count() == 2)
        #expect(try j2.select().map { $0 }.count == 2)
    }

    @Test func selectWhereNULL() throws {
        guard pgEnabled else { return }
        let db = try getTestDB()
        let t1 = db.table(TestTable1.self)
        let j1 = t1.where(\TestTable1.blob == nil)
        #expect(try j1.count() > 0)
        let j2 = t1.where(\TestTable1.blob != nil)
        #expect(try j2.count() > 0)
        CRUDLogging.flush()
    }

    @Test func personThing() throws {
        guard pgEnabled else { return }
        struct PhoneNumber: Codable {
            let personId: UUID
            let planetCode: Int
            let number: String
        }
        struct Person: Codable {
            let id: UUID
            let firstName: String
            let lastName: String
            let phoneNumbers: [PhoneNumber]?
        }
        let db = try getTestDB()
        try db.create(Person.self, policy: .reconcileTable)
        let personTable = db.table(Person.self)
        let numbersTable = db.table(PhoneNumber.self)
        try numbersTable.index(\.personId)
        let owen = Person(id: UUID(), firstName: "Owen", lastName: "Lars", phoneNumbers: nil)
        let beru = Person(id: UUID(), firstName: "Beru", lastName: "Lars", phoneNumbers: nil)
        try personTable.insert([owen, beru])
        try numbersTable.insert([
            PhoneNumber(personId: owen.id, planetCode: 12, number: "555-555-1212"),
            PhoneNumber(personId: owen.id, planetCode: 15, number: "555-555-2222"),
            PhoneNumber(personId: beru.id, planetCode: 12, number: "555-555-1212")])
        let query = try personTable
            .order(by: \.lastName, \.firstName)
            .join(\.phoneNumbers, on: \.id, equals: \.personId)
            .order(descending: \.planetCode)
            .where(\Person.lastName == "Lars" && \PhoneNumber.planetCode == 12)
            .select()
        for user in query {
            guard let numbers = user.phoneNumbers else { continue }
            for number in numbers { _ = number.number }
        }
        CRUDLogging.flush()
    }

    @Test func standardJoin() throws {
        guard pgEnabled else { return }
        struct Parent: Codable {
            let id: Int
            let children: [Child]?
            init(id i: Int) { id = i; children = nil }
        }
        struct Child: Codable {
            let id: Int
            let parentId: Int
        }
        let db = try getTestDB()
        try db.transaction {
            try db.create(Parent.self, policy: [.shallow, .dropTable]).insert(Parent(id: 1))
            try db.create(Child.self, policy: [.shallow, .dropTable]).insert(
                [Child(id: 1, parentId: 1), Child(id: 2, parentId: 1), Child(id: 3, parentId: 1)])
        }
        let join = try db.table(Parent.self)
            .join(\.children, on: \.id, equals: \.parentId)
            .where(\Parent.id == 1)
        let parent = try #require(try join.first())
        let children = try #require(parent.children)
        #expect(children.count == 3)
        for child in children { #expect(child.parentId == parent.id) }
        CRUDLogging.flush()
    }

    @Test func junctionJoin() throws {
        guard pgEnabled else { return }
        struct Student: Codable {
            let id: Int
            let classes: [Class]?
            init(id i: Int) { id = i; classes = nil }
        }
        struct Class: Codable {
            let id: Int
            let students: [Student]?
            init(id i: Int) { id = i; students = nil }
        }
        struct StudentClasses: Codable {
            let studentId: Int
            let classId: Int
        }
        let db = try getTestDB()
        try db.transaction {
            try db.create(Student.self, policy: [.dropTable, .shallow]).insert(Student(id: 1))
            try db.create(Class.self, policy: [.dropTable, .shallow]).insert([Class(id: 1), Class(id: 2), Class(id: 3)])
            try db.create(StudentClasses.self, policy: [.dropTable, .shallow]).insert([
                StudentClasses(studentId: 1, classId: 1),
                StudentClasses(studentId: 1, classId: 2),
                StudentClasses(studentId: 1, classId: 3)])
        }
        let join = try db.table(Student.self)
            .join(\.classes, with: StudentClasses.self, on: \.id, equals: \.studentId, and: \.id, is: \.classId)
            .where(\Student.id == 1)
        let student = try #require(try join.first())
        let classes = try #require(student.classes)
        #expect(classes.count == 3)
        for aClass in classes {
            let join = try db.table(Class.self)
                .join(\.students, with: StudentClasses.self, on: \.id, equals: \.classId, and: \.id, is: \.studentId)
                .where(\Class.id == aClass.id)
            let found = try #require(try join.first())
            #expect(found.students?.first(where: { $0.id == student.id }) != nil)
        }
        CRUDLogging.flush()
    }

    @Test func selfJoin() throws {
        guard pgEnabled else { return }
        struct Me: Codable {
            let id: Int
            let parentId: Int
            let mes: [Me]?
            init(id i: Int, parentId p: Int) { id = i; parentId = p; mes = nil }
        }
        let db = try getTestDB()
        try db.transaction { () -> Void in
            _ = try db.create(Me.self, policy: .dropTable).insert([
                Me(id: 1, parentId: 0), Me(id: 2, parentId: 1),
                Me(id: 3, parentId: 1), Me(id: 4, parentId: 1), Me(id: 5, parentId: 1)])
        }
        let join = try db.table(Me.self).join(\.mes, on: \.id, equals: \.parentId).where(\Me.id == 1)
        let me = try #require(try join.first())
        let mes = try #require(me.mes)
        #expect(mes.count == 4)
    }

    @Test func selfJunctionJoin() throws {
        guard pgEnabled else { return }
        struct Me: Codable {
            let id: Int
            let us: [Me]?
            init(id i: Int) { id = i; us = nil }
        }
        struct Us: Codable {
            let you: Int
            let them: Int
        }
        let db = try getTestDB()
        try db.transaction {
            try db.create(Me.self, policy: .dropTable).insert((1...5).map { .init(id: $0) })
            try db.create(Us.self, policy: .dropTable).insert((2...5).map { .init(you: 1, them: $0) })
        }
        let join = try db.table(Me.self)
            .join(\.us, with: Us.self, on: \.id, equals: \.you, and: \.id, is: \.them)
            .where(\Me.id == 1)
        let me = try #require(try join.first())
        let us = try #require(me.us)
        #expect(us.count == 4)
    }

    @Test func codableProperty() throws {
        guard pgEnabled else { return }
        struct Sub: Codable { let id: Int }
        struct Top: Codable { let id: Int; let sub: Sub? }
        let db = try getTestDB()
        try db.create(Sub.self)
        try db.create(Top.self)
        let t1 = Top(id: 1, sub: Sub(id: 1))
        try db.table(Top.self).insert(t1)
        let top = try #require(try db.table(Top.self).where(\Top.id == 1).first())
        #expect(top.sub?.id == t1.sub?.id)
    }

    @Test func badDecoding() throws {
        guard pgEnabled else { return }
        struct Top: Codable, TableNameProvider {
            static let tableName = "Top"
            let id: Int
        }
        struct NTop: Codable, TableNameProvider {
            static let tableName = "Top"
            let nid: Int
        }
        let db = try getTestDB()
        try db.create(Top.self, policy: .dropTable)
        _ = try db.table(Top.self).insert(Top(id: 1))
        #expect(throws: (any Error).self) {
            _ = try db.table(NTop.self).first()
        }
    }

    @Test func allPrimTypes1() throws {
        guard pgEnabled else { return }
        struct AllTypes: Codable {
            let int: Int; let uint: UInt; let int64: Int64; let uint64: UInt64
            let int32: Int32?; let uint32: UInt32?; let int16: Int16; let uint16: UInt16
            let int8: Int8?; let uint8: UInt8?; let double: Double; let float: Float
            let string: String; let bytes: [Int8]; let ubytes: [UInt8]?; let b: Bool
        }
        do {
            let db = try getTestDB()
            try db.create(AllTypes.self, policy: .dropTable)
            let model = AllTypes(int: 1, uint: 2, int64: 3, uint64: 4, int32: 5, uint32: 6,
                                 int16: 7, uint16: 8, int8: 9, uint8: 10, double: 11,
                                 float: 12, string: "13", bytes: [1, 4], ubytes: [1, 4], b: true)
            try db.table(AllTypes.self).insert(model)
            let f = try #require(try db.table(AllTypes.self).where(\AllTypes.int == 1).first())
            #expect(model.int == f.int); #expect(model.uint == f.uint)
            #expect(model.int64 == f.int64); #expect(model.uint64 == f.uint64)
            #expect(model.int32 == f.int32); #expect(model.uint32 == f.uint32)
            #expect(model.int16 == f.int16); #expect(model.uint16 == f.uint16)
            #expect(model.int8 == f.int8); #expect(model.uint8 == f.uint8)
            #expect(model.double == f.double); #expect(model.float == f.float)
            #expect(model.string == f.string); #expect(model.bytes == f.bytes)
            #expect(model.ubytes! == f.ubytes!); #expect(model.b == f.b)
        }
        do {
            let db = try getTestDB()
            try db.create(AllTypes.self, policy: .dropTable)
            let model = AllTypes(int: 1, uint: 2, int64: -3, uint64: 4, int32: nil, uint32: nil,
                                 int16: -7, uint16: 8, int8: nil, uint8: nil, double: -11,
                                 float: -12, string: "13", bytes: [1, 4], ubytes: nil, b: true)
            try db.table(AllTypes.self).insert(model)
            let f = try #require(try db.table(AllTypes.self).where(\AllTypes.int == 1).first())
            #expect(model.int == f.int); #expect(model.uint == f.uint)
            #expect(model.int64 == f.int64); #expect(model.uint64 == f.uint64)
            #expect(model.int32 == f.int32); #expect(model.uint32 == f.uint32)
            #expect(model.int16 == f.int16); #expect(model.uint16 == f.uint16)
            #expect(model.int8 == f.int8); #expect(model.uint8 == f.uint8)
            #expect(model.double == f.double); #expect(model.float == f.float)
            #expect(model.string == f.string); #expect(model.bytes == f.bytes)
            #expect(f.ubytes == nil); #expect(model.b == f.b)
        }
    }

    @Test func allPrimTypes2() throws {
        guard pgEnabled else { return }
        struct AllTypes2: Codable {
            let int: Int?; let uint: UInt?; let int64: Int64?; let uint64: UInt64?
            let int32: Int32?; let uint32: UInt32?; let int16: Int16?; let uint16: UInt16?
            let int8: Int8?; let uint8: UInt8?; let double: Double?; let float: Float?
            let string: String?; let bytes: [Int8]?; let ubytes: [UInt8]?; let b: Bool?
            func equals(rhs: AllTypes2) -> Bool {
                guard int == rhs.int && uint == rhs.uint &&
                    int64 == rhs.int64 && uint64 == rhs.uint64 &&
                    int32 == rhs.int32 && uint32 == rhs.uint32 &&
                    int16 == rhs.int16 && uint16 == rhs.uint16 &&
                    int8 == rhs.int8 && uint8 == rhs.uint8 &&
                    double == rhs.double && float == rhs.float &&
                    string == rhs.string && b == rhs.b else { return false }
                guard (bytes == nil) == (rhs.bytes == nil) else { return false }
                guard (ubytes == nil) == (rhs.ubytes == nil) else { return false }
                if let lhsb = bytes { guard lhsb == rhs.bytes! else { return false } }
                if let lhsb = ubytes { guard lhsb == rhs.ubytes! else { return false } }
                return true
            }
        }
        let db = try getTestDB()
        try db.create(AllTypes2.self, policy: .dropTable)
        let model = AllTypes2(int: 1, uint: 2, int64: -3, uint64: 4, int32: 5, uint32: 6,
                               int16: 7, uint16: 8, int8: 9, uint8: 10,
                               double: 11.2, float: 12.3, string: "13",
                               bytes: [1, 4], ubytes: [1, 4], b: true)
        try db.table(AllTypes2.self).insert(model)
        let f1 = try #require(try db.table(AllTypes2.self)
            .where(\AllTypes2.int == 1 && \AllTypes2.uint == 2 && \AllTypes2.int64 == -3).first())
        #expect(model.equals(rhs: f1))
        #expect(try db.table(AllTypes2.self)
            .where(\AllTypes2.int != 1 && \AllTypes2.uint != 2 && \AllTypes2.int64 != -3).count() == 0)
        let f2 = try #require(try db.table(AllTypes2.self)
            .where(\AllTypes2.uint64 == 4 && \AllTypes2.int32 == 5 && \AllTypes2.uint32 == 6).first())
        #expect(model.equals(rhs: f2))
        let f3 = try #require(try db.table(AllTypes2.self)
            .where(\AllTypes2.int16 == 7 && \AllTypes2.uint16 == 8 && \AllTypes2.int8 == 9 && \AllTypes2.uint8 == 10).first())
        #expect(model.equals(rhs: f3))
        let f4 = try #require(try db.table(AllTypes2.self)
            .where(\AllTypes2.double == 11.2 && \AllTypes2.float == Float(12.3) && \AllTypes2.string == "13").first())
        #expect(model.equals(rhs: f4))
        let f5 = try #require(try db.table(AllTypes2.self)
            .where(\AllTypes2.bytes == [1, 4] as [Int8] && \AllTypes2.ubytes == [1, 4] as [UInt8] && \AllTypes2.b == true).first())
        #expect(model.equals(rhs: f5))
    }

    @Test func bespokeSQL() throws {
        guard pgEnabled else { return }
        let db = try getTestDB()
        let r1 = try db.sql("SELECT * FROM \(TestTable1.CRUDTableName) WHERE id = 2", TestTable1.self)
        #expect(r1.count == 1)
        let r2 = try db.sql("SELECT * FROM \(TestTable1.CRUDTableName)", TestTable1.self)
        #expect(r2.count == 5)
    }

    @Test func modelClasses() throws {
        guard pgEnabled else { return }
        class BaseClass: Codable {
            let id: Int
            let name: String
            private enum CodingKeys: String, CodingKey { case id, name }
            init(id: Int, name: String) { self.id = id; self.name = name }
            required init(from decoder: Decoder) throws {
                let container = try decoder.container(keyedBy: CodingKeys.self)
                id = try container.decode(Int.self, forKey: .id)
                name = try container.decode(String.self, forKey: .name)
            }
            func encode(to encoder: Encoder) throws {
                var container = encoder.container(keyedBy: CodingKeys.self)
                try container.encode(id, forKey: .id)
                try container.encode(name, forKey: .name)
            }
        }
        class SubClass: BaseClass {
            let another: String
            private enum CodingKeys: String, CodingKey { case another }
            init(id: Int, name: String, another: String) {
                self.another = another
                super.init(id: id, name: name)
            }
            required init(from decoder: Decoder) throws {
                let container = try decoder.container(keyedBy: CodingKeys.self)
                another = try container.decode(String.self, forKey: .another)
                try super.init(from: decoder)
            }
            override func encode(to encoder: Encoder) throws {
                var container = encoder.container(keyedBy: CodingKeys.self)
                try container.encode(another, forKey: .another)
                try super.encode(to: encoder)
            }
        }
        let db = try getTestDB()
        try db.create(SubClass.self)
        let table = db.table(SubClass.self)
        let obj = SubClass(id: 1, name: "The name", another: "And another thing")
        try table.insert(obj)
        let found = try #require(try table.where(\SubClass.id == 1).first())
        #expect(found.another == obj.another)
        #expect(found.name == obj.name)
    }

    @Test func url() throws {
        guard pgEnabled else { return }
        struct TableWithURL: Codable { let id: Int; let url: URL }
        let db = try getTestDB()
        try db.create(TableWithURL.self)
        let t1 = db.table(TableWithURL.self)
        let newOne = TableWithURL(id: 2000, url: URL(string: "http://localhost/")!)
        try t1.insert(newOne)
        let j1 = t1.where(\TableWithURL.id == newOne.id)
        let j2 = try j1.select().map { $0 }
        #expect(try j1.count() == 1)
        #expect(j2[0].id == 2000)
        #expect(j2[0].url.absoluteString == "http://localhost/")
    }

    @Test func manyJoins() throws {
        guard pgEnabled else { return }
        struct Person2: Codable { var id: UUID; var name: String; let cars: [Car]?; let boats: [Boat]?; let houses: [House]? }
        struct Car: Codable { var id: UUID; var owner: UUID }
        struct Boat: Codable { var id: UUID; var owner: UUID }
        struct House: Codable { var id: UUID; var owner: UUID }
        let db = try getTestDB()
        try db.create(Person2.self)
        try db.table(Car.self).index(\.owner)
        try db.table(Boat.self).index(\.owner)
        try db.table(House.self).index(\.owner)
        let t1 = db.table(Person2.self)
        let parentId = UUID()
        try t1.insert(Person2(id: parentId, name: "The Person", cars: nil, boats: nil, houses: nil))
        for _ in 0..<5 {
            try db.table(Car.self).insert(.init(id: UUID(), owner: parentId))
            try db.table(Boat.self).insert(.init(id: UUID(), owner: parentId))
            try db.table(House.self).insert(.init(id: UUID(), owner: parentId))
        }
        let j1 = try t1.join(\.cars, on: \.id, equals: \.owner)
            .join(\.boats, on: \.id, equals: \.owner)
            .join(\.houses, on: \.id, equals: \.owner)
            .where(\Person2.id == parentId)
        let j2 = try #require(try j1.first())
        #expect(j2.cars?.count == 5)
        #expect(j2.boats?.count == 5)
        #expect(j2.houses?.count == 5)
    }

    @Test func dateFormat() {
        #expect(Date(fromISO8601: "2018-08-18 08:10:51-04") != nil)
        #expect(Date(fromISO8601: "2018-08-18 08:10:51.32-04:00") != nil)
        #expect(Date(fromISO8601: "2018-08-18 08:10:51Z") != nil)
        #expect(Date(fromISO8601: "2018-08-18 08:10:51.43Z") != nil)
    }

    @Test func assets() throws {
        guard pgEnabled else { return }
        struct Asset: Codable {
            let id: UUID; let name: String?; let assetLog: [AssetLog]?
            init(id: UUID, name: String? = nil, assetLog: [AssetLog]? = nil) {
                self.id = id; self.name = name; self.assetLog = assetLog
            }
        }
        struct AssetLog: Codable {
            let assetId: UUID; let userId: UUID; let taken: Double; let returned: Double?
            init(assetId: UUID, userId: UUID, taken: Double, returned: Double? = nil) {
                self.assetId = assetId; self.userId = userId; self.taken = taken; self.returned = returned
            }
        }
        let db = try getTestDB()
        try db.create(Asset.self, policy: .dropTable)
        let id = UUID(); let userId = UUID()
        try db.table(Asset.self).insert(Asset(id: id, name: "name"))
        try db.table(AssetLog.self).insert([
            AssetLog(assetId: id, userId: userId, taken: 1.0),
            AssetLog(assetId: id, userId: userId, taken: 2.0)])
        let asset = try db.table(Asset.self)
            .join(\.assetLog, on: \.id, equals: \.assetId)
            .where(\AssetLog.userId == userId && \AssetLog.returned == nil)
            .first()
        #expect(asset?.assetLog != nil)
        #expect(asset?.id == id)
        #expect(asset?.assetLog?.count == 2)
    }

    @Test func returningInsert() throws {
        guard pgEnabled else { return }
        struct ReturningItem: Codable, Equatable {
            let id: UUID; let def: Int?
            init(id: UUID, def: Int? = nil) { self.id = id; self.def = def }
        }
        let db = try getTestDB()
        try db.sql("DROP TABLE IF EXISTS \(ReturningItem.CRUDTableName)")
        try db.sql("CREATE TABLE \(ReturningItem.CRUDTableName) (id UUID PRIMARY KEY, def int DEFAULT 42)")
        let table = db.table(ReturningItem.self)
        let def1 = try table.returning(\.def, insert: ReturningItem(id: UUID()), ignoreKeys: \.def)
        #expect(def1 == 42)
        let defs = try table.returning(\.def,
            insert: [ReturningItem(id: UUID()), ReturningItem(id: UUID()), ReturningItem(id: UUID())],
            ignoreKeys: \.def)
        #expect(defs == [42, 42, 42])
        let id = UUID()
        let id0 = try table.returning(\.id, insert: ReturningItem(id: id, def: 42))
        #expect(id0 == id)
        let items = [ReturningItem(id: UUID()), ReturningItem(id: UUID()), ReturningItem(id: UUID())]
        let returned = try table.returning(insert: items, ignoreKeys: \.def)
        #expect(returned.map { $0.id } == items.map { $0.id })
        #expect(returned.compactMap { $0.def }.count == returned.count)
    }

    @Test func returningUpdate() throws {
        guard pgEnabled else { return }
        struct ReturningItem: Codable, Equatable {
            let id: UUID; var def: Int?
            init(id: UUID, def: Int? = nil) { self.id = id; self.def = def }
        }
        let db = try getTestDB()
        try db.sql("DROP TABLE IF EXISTS \(ReturningItem.CRUDTableName)")
        try db.sql("CREATE TABLE \(ReturningItem.CRUDTableName) (id UUID PRIMARY KEY, def int DEFAULT 42)")
        let table = db.table(ReturningItem.self)
        let id = UUID()
        var item = ReturningItem(id: id)
        try table.insert(item, ignoreKeys: \.def)
        item.def = 300
        let item0 = try table
            .where(\ReturningItem.id == id)
            .returning(\.def, update: item, ignoreKeys: \.id)
        #expect(item0.count == 1)
        #expect(item.def == item0.first)
    }

    @Test func emptyInsert() throws {
        guard pgEnabled else { return }
        struct ReturningItem: Codable, Equatable {
            let id: Int?; var def: Int?
            init(id: Int, def: Int? = nil) { self.id = id; self.def = def }
        }
        let db = try getTestDB()
        try db.sql("DROP TABLE IF EXISTS \(ReturningItem.CRUDTableName)")
        try db.sql("CREATE TABLE \(ReturningItem.CRUDTableName) (id SERIAL PRIMARY KEY, def INT DEFAULT 42)")
        let table = db.table(ReturningItem.self)
        try table.insert(ReturningItem(id: 0, def: 0), ignoreKeys: \ReturningItem.id, \ReturningItem.def)
        _ = try table.returning(\.def, insert: ReturningItem(id: 0, def: 0),
                                 ignoreKeys: \ReturningItem.id, \ReturningItem.def)
    }
}
