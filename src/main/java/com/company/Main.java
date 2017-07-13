package com.company;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.PCollection;

/**
 * メインクラス
 * Created by sekiguchikai on 2017/07/13.
 */
public class Main {
    /**
     * 関数型オブジェクト
     * String => Integerの型変換を行う
     */
    static class TransformTypeFromStringToInteger extends DoFn<String, Integer> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            // 要素をString=>Integerに変換して、output
            c.output(Integer.parseInt(c.element()));
        }
    }

    /**
     * 関数型オブジェクト
     * Integer =>Stringの型変換を行う
     */
    static class TransformTypeFromIntegerToString extends DoFn<Integer, String> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            // 要素をString=>Integerに変換して、output
            System.out.println(c.element());
            c.output(String.valueOf(c.element()));
        }
    }


    /**
     * インプットデータのパス
     */
    private static final String INPUT_FILE_PATH = "./sample.txt";
    /**
     * アウトデータのパス
     */
    private static final String OUTPUT_FILE_PATH = "./result.txt";

    /**
     * 理解のためにメソッドチェーンは極力使用しない
     * そのため冗長な箇所がある
     * メインメソッド
     *
     * @param args
     */
    public static void main(String[] args) {
        // optionを指定して、Pipelineを生成する
        Pipeline pipeline = Pipeline.create(PipelineOptionsFactory.create());

        System.out.println("a");
        // ファイルから読み込み
        PCollection<String> lines = pipeline.apply(TextIO.read().from(INPUT_FILE_PATH));
        // 読み込んだ各データをString => Integerに変換
        PCollection<Integer> integerPCollection = lines.apply(ParDo.of(new TransformTypeFromStringToInteger()));
        // Combine.GloballyでPCollectionの各要素を合計
        // 空のPCollectionの場合、emptyを返したいなら => PCollection<Integer> sum = integerPCollection.apply(Sum.integersGlobally().withoutDefaults());
        PCollection<Integer> sum = integerPCollection.apply(Sum.integersGlobally().withoutDefaults());
        // PCollection<Integer> sumをInteger => Stringに変換
        PCollection<String> sumString = sum.apply(ParDo.of(new TransformTypeFromIntegerToString()));
        // ファイルに書き込み
        sumString.apply(TextIO.write().to(OUTPUT_FILE_PATH));

        // 実行
        pipeline.run().waitUntilFinish();
    }
}
