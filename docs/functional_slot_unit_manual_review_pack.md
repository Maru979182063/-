# Functional Slot Unit Manual Review Pack

## 1. 本包说明

这是一版专门用于人工查验 `functional_slot_unit` 的测试包。

本包样本来自当前仓库、当前代码、当前 `MaterialPipelineV2` 的真实 planner 结果，不是手工编造。

抽取口径：

- 只看当前新增的中性承载层：`functional_slot_unit`
- 基于当前文章上下文跑 `V2 rule functional slot builder`
- 再经过当前 planner 的分数筛选
- 不混入旧的 `multi_paragraph_unit` 对照项

当前这版包主要覆盖到的 slot 角色/功能：

- `opening / topic_intro`
- `middle / lead_next`
- `ending / ending_summary`
- `ending / countermeasure`

当前这版包里还没有稳定抽到的功能位：

- `opening / summary`
- `middle / carry_previous`
- `middle / bridge_both_sides`

这三类不是说设计上没有，而是当前样本里还没有稳定产出到足够像正式承载单元的结果。

## 2. 当前人工查验重点

建议人工重点看这几件事：

- 它是否已经像“局部功能位单元”，而不只是普通大段正文
- `slot_role / slot_function` 是否和正文观感一致
- 去掉该位后，是否真的有 blank 空位价值
- 前后关系是否仍然能判断
- 它是否仍然过宽、过整段、过说明化

## 3. 样本清单

### Sample 1

- `article_id`: `article_c0d4cc4d53f44bc183606c0963771fe2`
- `title`: 坚持唯物辩证法 践行正确政绩观
- `unit_type`: `functional_slot_unit`
- `slot_role`: `opening`
- `slot_function`: `topic_intro`
- `paragraph_range`: `[0, 1]`
- `slot_sentence_range`: `[0, 0]`
- `planner_score`: `0.8881`
- `generated_by`: `v2_primary_candidate_builder+rule_functional_slot_builder+slot_role=opening+slot_function=topic_intro+slot_sentence=0-0`

正文：

政绩观问题是一个根本性问题，关乎立党为公、执政为民。树立和践行正确政绩观，必须回答好“政绩为谁而树、树什么样的政绩、靠什么树政绩”的问题，始终坚持人民至上，把为民办事、为民造福作为最重要的政绩。习近平总书记强调：“要学习掌握唯物辩证法的根本方法，不断增强辩证思维能力，提高驾驭复杂局面、处理复杂问题的本领。”这深刻揭示了政绩观与科学方法论的内在逻辑——正确政绩观的树立，离不开唯物辩证法的科学指引；辩证思维能力的提升，是践行正确政绩观的必然要求。新时代新征程，面对改革发展稳定繁重任务，必须以唯物辩证法为指导，统筹兼顾，重点处理好显绩与潜绩、当下与长远、发展和稳定、发展和民生、发展和人心的紧密联系，创造经得起实践、人民和历史检验的实绩。

坚持对立统一规律，处理好显绩与潜绩的关系。任何事物都是矛盾的对立统一体，既相互区别又相互联系。显绩是看得见、摸得着、得实惠的现实成效，具有直观性、时效性和普惠性；潜绩是打基础、利长远、作铺垫的基础性工作，往往周期长、见效慢、不易显现。二者相辅相成，潜绩是显绩的根基，显绩是潜绩的外在体现，正确政绩观要求科学把握二者关系，坚决防止片面化、绝对化。

---

### Sample 2

- `article_id`: `article_c0d4cc4d53f44bc183606c0963771fe2`
- `title`: 坚持唯物辩证法 践行正确政绩观
- `unit_type`: `functional_slot_unit`
- `slot_role`: `middle`
- `slot_function`: `lead_next`
- `paragraph_range`: `[3, 4]`
- `slot_sentence_range`: `[15, 15]`
- `planner_score`: `0.8381`
- `generated_by`: `v2_primary_candidate_builder+rule_functional_slot_builder+slot_role=middle+slot_function=lead_next+slot_sentence=15-15`

正文：

遵循质量互变规律，处理好当下见效与长远奠基的关系。事物发展是量变与质变的统一，既要重视量的积累、推动质的提升，也要立足当前解决突出问题、着眼长远实现战略目标。当下见效是回应群众期盼、稳定发展大局、凝聚干事信心的现实需要，是推动事业前进的必要量变积累；长远奠基是把握发展大势、遵循客观规律、实现可持续发展的战略要求，是高质量发展的质变准备。只重当下不顾长远，易透支资源、积累风险；只讲长远轻视当下，会失去现实支撑，难以凝聚共识、激发动力。

正确处理当下与长远，关键在于循序渐进、接续发力，防止急躁冒进和消极等待。一些地方在生态治理、教育提质、科技攻关中，将年度任务与长远规划相结合，以阶段性成效夯实长远基础，以长远目标引领当下工作，实现循序渐进、梯次跃升。党员干部要树立正确事业观、发展观，以“时时放心不下”的责任感抓好当前工作，以“久久为功”的韧劲做好铺垫性、基础性工作，在量变积累中推动事业实现质的飞跃，真正功在当代、利在千秋。

---

### Sample 3

- `article_id`: `article_c0d4cc4d53f44bc183606c0963771fe2`
- `title`: 坚持唯物辩证法 践行正确政绩观
- `unit_type`: `functional_slot_unit`
- `slot_role`: `ending`
- `slot_function`: `ending_summary`
- `paragraph_range`: `[5, 6]`
- `slot_sentence_range`: `[24, 24]`
- `planner_score`: `0.8622`
- `generated_by`: `v2_primary_candidate_builder+rule_functional_slot_builder+slot_role=ending+slot_function=ending_summary+slot_sentence=24-24`

正文：

坚持普遍联系观点，处理好发展和稳定、发展和民生、发展和人心的紧密联系。事物是普遍联系的，发展是系统工程，必须统筹兼顾、整体推进，防止单打一、简单化。发展是党执政兴国的第一要务，民生是最大的政治，稳定是发展的前提，统筹高质量发展和高水平安全是为了更好惠民生、保稳定，惠民生、保稳定又为发展创造良好环境。新时代以来，从脱贫攻坚到乡村振兴，从民生保障到基层治理，从风险防范到化解积案，充分体现系统观念和统筹思维。各地坚持发展为民、惠民，在推动经济转型升级的同时，着力解决就业、教育、医疗、养老等民生问题，守住安全稳定底线；坚持新官要理旧账，以法治思维和务实举措破解历史遗留问题，实现当前与长远、发展与安全、效率与公平的协调统一。这启示我们，践行正确政绩观，必须坚持系统观念，强化统筹意识，把抓发展与惠民生、保稳定结合起来，把破解现实难题与克服历史积弊统一起来，推动各项工作协同发力。

（作者：张云龙，系西北工业大学马克思主义学院副院长、教授）

---

### Sample 4

- `article_id`: `article_0c4a3089a6334cf6b0f34d52fc2105bc`
- `title`: 中国经济的强大韧性是高质量发展的有力支撑
- `unit_type`: `functional_slot_unit`
- `slot_role`: `opening`
- `slot_function`: `topic_intro`
- `paragraph_range`: `[0, 1]`
- `slot_sentence_range`: `[0, 0]`
- `planner_score`: `0.8315`
- `generated_by`: `v2_primary_candidate_builder+rule_functional_slot_builder+slot_role=opening+slot_function=topic_intro+slot_sentence=0-0`

正文：

习近平总书记反复强调我国经济韧性强。当前，世界百年未有之大变局加速演进，各类风险挑战交织叠加。面对复杂严峻的外部环境与国内改革发展稳定的艰巨任务，经济韧性已成为衡量我国经济成熟度、竞争力与安全性的重要指标，是高质量发展和高水平安全良性互动的集中体现，是应对外部冲击、把握发展主动权、实现长期稳定发展的关键支撑。深刻把握习近平总书记关于经济韧性的重要论述的丰富内涵和实践要求，对于推动高质量发展，以中国式现代化全面推进强国建设、民族复兴伟业具有重大而深远的意义。

习近平总书记关于经济韧性的重要论述具有鲜明的原创性

---

### Sample 5

- `article_id`: `article_0c4a3089a6334cf6b0f34d52fc2105bc`
- `title`: 中国经济的强大韧性是高质量发展的有力支撑
- `unit_type`: `functional_slot_unit`
- `slot_role`: `ending`
- `slot_function`: `ending_summary`
- `paragraph_range`: `[21, 22]`
- `slot_sentence_range`: `[95, 95]`
- `planner_score`: `0.7613`
- `generated_by`: `v2_primary_candidate_builder+rule_functional_slot_builder+slot_role=ending+slot_function=ending_summary+slot_sentence=95-95`

正文：

进一步全面深化改革，强化制度供给。改革是发展的动力，进一步全面深化改革是在新时代以来全面深化改革基础上推进的。要深化重点领域和关键环节改革，完善社会主义市场经济体制。优化民营经济发展环境，保障各类所有制企业公平参与市场竞争。深化财税、金融、国企、要素市场化配置等改革，以制度创新提升治理效能，为增强经济韧性提供持久制度保障。

（作者为习近平经济思想研究中心副主任）

---

### Sample 6

- `article_id`: `article_5fa39d8d6cca4684a9f495c25ef30afe`
- `title`: 破解时代之问的中国方略
- `unit_type`: `functional_slot_unit`
- `slot_role`: `ending`
- `slot_function`: `countermeasure`
- `paragraph_range`: `[45, 46]`
- `slot_sentence_range`: `[125, 125]`
- `planner_score`: `0.7767`
- `generated_by`: `v2_primary_candidate_builder+rule_functional_slot_builder+slot_role=ending+slot_function=countermeasure+slot_sentence=125-125`

正文：

经济日报积极建设学习宣传研究习近平经济思想高地，举行2025年度学思践悟习近平经济思想丛书出版座谈会后，海外平台积极对外宣介座谈会嘉宾发言，推出《推动党的创新理论“飞入寻常百姓家”》《从三个维度理解科学理论》《推动学习研究宣传习近平经济思想持续深入》《加强对经济发展的经验总结和理论研究》等图文报道，广受关注，阅读量达519万，互动超3400次。

习近平总书记致经济日报创刊40周年贺信指出，要为推动中国经济高质量发展、讲好新时代中国经济发展故事作出新的更大贡献。我们将牢记贺信嘱托，不断提高运用习近平新时代中国特色社会主义思想指导实践、推动工作的能力和水平，切实肩负起联接中外、沟通世界的重要职责，着力创新经济报道理念和方式，提高经济报道质量和水平，努力向世界讲好新时代中国经济发展故事，充分展现奋力开创中国式现代化建设新局面的时代价值和世界意义。

---

### Sample 7

- `article_id`: `article_f541a85fd3c04f0885deaf8da6d35340`
- `title`: 多地春假陆续落地 有望激发消费市场新增量
- `unit_type`: `functional_slot_unit`
- `slot_role`: `opening`
- `slot_function`: `topic_intro`
- `paragraph_range`: `[0, 1]`
- `slot_sentence_range`: `[0, 0]`
- `planner_score`: `0.8257`
- `generated_by`: `v2_primary_candidate_builder+rule_functional_slot_builder+slot_role=opening+slot_function=topic_intro+slot_sentence=0-0`

正文：

今年清明假期，不少地方按照“十五五”规划纲要提出的“探索推行中小学生春秋假”，积极落地春假与清明假期的协同。更长的假期不仅带动旅游市场进一步增长，也为旅游产品的进一步迭代升级注入活力。业内专家表示，假期的增加对于消费文旅市场有着积极作用，各地需结合自身情况持续探索。

“这个学期开学收到学校放春假的通知，恰逢旅游淡季，我们决定家庭自行出游。从体验来看，错峰出游避开了节假日人潮拥挤，省下不少出行开支。首先机票价格直接腰斩，大幅节省了交通成本。住宿更是优势显著，无论是酒店或民宿价格都仅为黄金周的三分之一，能轻松订到宽敞舒适的家庭房，大大提升了出游体验，也让我们一家度过了愉快的亲子时光。”广州市玉岩中学一位学生家长告诉记者。

---

### Sample 8

- `article_id`: `article_f541a85fd3c04f0885deaf8da6d35340`
- `title`: 多地春假陆续落地 有望激发消费市场新增量
- `unit_type`: `functional_slot_unit`
- `slot_role`: `middle`
- `slot_function`: `lead_next`
- `paragraph_range`: `[1, 2]`
- `slot_sentence_range`: `[8, 8]`
- `planner_score`: `0.7498`
- `generated_by`: `v2_primary_candidate_builder+rule_functional_slot_builder+slot_role=middle+slot_function=lead_next+slot_sentence=8-8`

正文：

“这个学期开学收到学校放春假的通知，恰逢旅游淡季，我们决定家庭自行出游。从体验来看，错峰出游避开了节假日人潮拥挤，省下不少出行开支。首先机票价格直接腰斩，大幅节省了交通成本。住宿更是优势显著，无论是酒店或民宿价格都仅为黄金周的三分之一，能轻松订到宽敞舒适的家庭房，大大提升了出游体验，也让我们一家度过了愉快的亲子时光。”广州市玉岩中学一位学生家长告诉记者。

广州市玉岩中学副校长李舜表示，结合去年放春秋假的经验，今年学校进一步优化了春假安排，把春假和清明节假期连在一起放。“从落地情况来看，今年玉岩春假一个亮点是，选择家庭出游的人数显著增多。根据家长和学生反馈，对于春秋假都给出了良好评价。未来学校不仅会继续放春秋假，还会继续积极探索用更好的方式去放假。”

## 4. 当前空缺位说明

这次测试包里暂未纳入以下功能位样本：

- `opening / summary`
- `middle / carry_previous`
- `middle / bridge_both_sides`

当前更像是：

- planner 设计已允许这些功能位
- 但当前样本文章里，还没有稳定跑出足够像正式承载单元的结果

这部分建议人工查验时一并注意：

- 当前 `opening / topic_intro` 是否过度替代了 `opening / summary`
- 当前 `middle / lead_next` 是否过度吞掉了 `carry_previous / bridge_both_sides`
- 当前 `ending / countermeasure` 是否已经和 `ending / ending_summary` 拉开了足够清楚的边界

## 5. 本包用途

这版包不是最终业务卡评估包，而是：

- `functional_slot_unit` 是否已经开始像“正式中性承载层”的第一轮人工查验包

建议人工查验后重点反馈：

- 哪些样本已经明显像可 blank 的功能位单元
- 哪些样本仍然只是“带 slot 标签的普通大段材料”
- 哪些功能位当前边界仍然太松
- 哪些功能位当前仍然缺样本
