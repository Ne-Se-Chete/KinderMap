import { query } from "sdk/db";
import { producer } from "sdk/messaging";
import { extensions } from "sdk/extensions";
import { dao as daoApi } from "sdk/db";
import { EntityUtils } from "../utils/EntityUtils";

export interface AcceptanceRateEntity {
    readonly Id: number;
    Date?: Date;
    KinderGarden?: number;
    Capacity?: number;
    AppliedChildren?: number;
    AcceptedChildren?: number;
}

export interface AcceptanceRateCreateEntity {
    readonly Date?: Date;
    readonly KinderGarden?: number;
    readonly Capacity?: number;
    readonly AppliedChildren?: number;
    readonly AcceptedChildren?: number;
}

export interface AcceptanceRateUpdateEntity extends AcceptanceRateCreateEntity {
    readonly Id: number;
}

export interface AcceptanceRateEntityOptions {
    $filter?: {
        equals?: {
            Id?: number | number[];
            Date?: Date | Date[];
            KinderGarden?: number | number[];
            Capacity?: number | number[];
            AppliedChildren?: number | number[];
            AcceptedChildren?: number | number[];
        };
        notEquals?: {
            Id?: number | number[];
            Date?: Date | Date[];
            KinderGarden?: number | number[];
            Capacity?: number | number[];
            AppliedChildren?: number | number[];
            AcceptedChildren?: number | number[];
        };
        contains?: {
            Id?: number;
            Date?: Date;
            KinderGarden?: number;
            Capacity?: number;
            AppliedChildren?: number;
            AcceptedChildren?: number;
        };
        greaterThan?: {
            Id?: number;
            Date?: Date;
            KinderGarden?: number;
            Capacity?: number;
            AppliedChildren?: number;
            AcceptedChildren?: number;
        };
        greaterThanOrEqual?: {
            Id?: number;
            Date?: Date;
            KinderGarden?: number;
            Capacity?: number;
            AppliedChildren?: number;
            AcceptedChildren?: number;
        };
        lessThan?: {
            Id?: number;
            Date?: Date;
            KinderGarden?: number;
            Capacity?: number;
            AppliedChildren?: number;
            AcceptedChildren?: number;
        };
        lessThanOrEqual?: {
            Id?: number;
            Date?: Date;
            KinderGarden?: number;
            Capacity?: number;
            AppliedChildren?: number;
            AcceptedChildren?: number;
        };
    },
    $select?: (keyof AcceptanceRateEntity)[],
    $sort?: string | (keyof AcceptanceRateEntity)[],
    $order?: 'asc' | 'desc',
    $offset?: number,
    $limit?: number,
}

interface AcceptanceRateEntityEvent {
    readonly operation: 'create' | 'update' | 'delete';
    readonly table: string;
    readonly entity: Partial<AcceptanceRateEntity>;
    readonly key: {
        name: string;
        column: string;
        value: number;
    }
}

interface AcceptanceRateUpdateEntityEvent extends AcceptanceRateEntityEvent {
    readonly previousEntity: AcceptanceRateEntity;
}

export class AcceptanceRateRepository {

    private static readonly DEFINITION = {
        table: "ACCEPTANCERATE",
        properties: [
            {
                name: "Id",
                column: "ACCEPTANCERATE_ID",
                type: "INTEGER",
                id: true,
                autoIncrement: true,
            },
            {
                name: "Date",
                column: "ACCEPTANCERATE_DATE",
                type: "DATE",
            },
            {
                name: "KinderGarden",
                column: "ACCEPTANCERATE_KINDERGARDEN",
                type: "INTEGER",
            },
            {
                name: "Capacity",
                column: "ACCEPTANCERATE_CAPACITY",
                type: "INTEGER",
            },
            {
                name: "AppliedChildren",
                column: "ACCEPTANCERATE_APPLIEDCHILDREN",
                type: "INTEGER",
            },
            {
                name: "AcceptedChildren",
                column: "ACCEPTANCERATE_ACCEPTEDCHILDREN",
                type: "INTEGER",
            }
        ]
    };

    private readonly dao;

    constructor(dataSource = "DefaultDB") {
        this.dao = daoApi.create(AcceptanceRateRepository.DEFINITION, null, dataSource);
    }

    public findAll(options?: AcceptanceRateEntityOptions): AcceptanceRateEntity[] {
        return this.dao.list(options).map((e: AcceptanceRateEntity) => {
            EntityUtils.setDate(e, "Date");
            return e;
        });
    }

    public findById(id: number): AcceptanceRateEntity | undefined {
        const entity = this.dao.find(id);
        EntityUtils.setDate(entity, "Date");
        return entity ?? undefined;
    }

    public create(entity: AcceptanceRateCreateEntity): number {
        EntityUtils.setLocalDate(entity, "Date");
        const id = this.dao.insert(entity);
        this.triggerEvent({
            operation: "create",
            table: "ACCEPTANCERATE",
            entity: entity,
            key: {
                name: "Id",
                column: "ACCEPTANCERATE_ID",
                value: id
            }
        });
        return id;
    }

    public update(entity: AcceptanceRateUpdateEntity): void {
        // EntityUtils.setLocalDate(entity, "Date");
        const previousEntity = this.findById(entity.Id);
        this.dao.update(entity);
        this.triggerEvent({
            operation: "update",
            table: "ACCEPTANCERATE",
            entity: entity,
            previousEntity: previousEntity,
            key: {
                name: "Id",
                column: "ACCEPTANCERATE_ID",
                value: entity.Id
            }
        });
    }

    public upsert(entity: AcceptanceRateCreateEntity | AcceptanceRateUpdateEntity): number {
        const id = (entity as AcceptanceRateUpdateEntity).Id;
        if (!id) {
            return this.create(entity);
        }

        const existingEntity = this.findById(id);
        if (existingEntity) {
            this.update(entity as AcceptanceRateUpdateEntity);
            return id;
        } else {
            return this.create(entity);
        }
    }

    public deleteById(id: number): void {
        const entity = this.dao.find(id);
        this.dao.remove(id);
        this.triggerEvent({
            operation: "delete",
            table: "ACCEPTANCERATE",
            entity: entity,
            key: {
                name: "Id",
                column: "ACCEPTANCERATE_ID",
                value: id
            }
        });
    }

    public count(options?: AcceptanceRateEntityOptions): number {
        return this.dao.count(options);
    }

    public customDataCount(): number {
        const resultSet = query.execute('SELECT COUNT(*) AS COUNT FROM "ACCEPTANCERATE"');
        if (resultSet !== null && resultSet[0] !== null) {
            if (resultSet[0].COUNT !== undefined && resultSet[0].COUNT !== null) {
                return resultSet[0].COUNT;
            } else if (resultSet[0].count !== undefined && resultSet[0].count !== null) {
                return resultSet[0].count;
            }
        }
        return 0;
    }

    private async triggerEvent(data: AcceptanceRateEntityEvent | AcceptanceRateUpdateEntityEvent) {
        const triggerExtensions = await extensions.loadExtensionModules("kinder-map-entities-AcceptanceRate", ["trigger"]);
        triggerExtensions.forEach(triggerExtension => {
            try {
                triggerExtension.trigger(data);
            } catch (error) {
                console.error(error);
            }            
        });
        producer.topic("kinder-map-entities-AcceptanceRate").send(JSON.stringify(data));
    }
}
